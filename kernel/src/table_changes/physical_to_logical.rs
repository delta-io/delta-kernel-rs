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
    // Build partition values separately
    let partition_field_values =
        parse_partition_values_to_expressions(logical_schema, transform_spec, partition_values)?;

    // Build CDF field values separately and note which field we need to add the `_change_type` column after
    let mut cdf_field_values = HashMap::new();
    let mut change_type_insert_after = None;
    for field_transform in transform_spec {
        if let FieldTransformSpec::Cdf {
            col_type,
            field_index,
            insert_after,
        } = field_transform
        {
            let (_, field) = logical_schema
                .fields
                .get_index(*field_index)
                .ok_or_else(|| {
                    crate::Error::InternalError(format!("out of bounds field index {field_index}"))
                })?;

            // If the CDF expression is not None, insert it into the cdf_field_values map
            // If it is None, it means the CDF column should come from physical data
            if let Some(expr) = create_cdf_expression(col_type, scan_file) {
                cdf_field_values.insert(*field_index, (field.name().to_string(), expr));
            }

            // Set change_type_insert_after for non-CDC scans to redirect columns that target the change type
            //
            // Example: In test_column_ordering_with_change_type:
            // - ChangeType CDF has insert_after = "name" (where it should go)
            // - CommitVersion CDF has insert_after = "_change_type" (wants to go after change type)
            // - We set change_type_insert_after = "name" so CommitVersion gets redirected to "name"
            // - Result: Both ChangeType and CommitVersion go after "name" in correct order
            //
            // For CDC scans: change_type column exists in physical data, so no redirection needed
            if scan_file.scan_type != CdfScanFileType::Cdc
                && matches!(col_type, crate::scan::CdfCol::ChangeType(_))
            {
                change_type_insert_after = insert_after.clone();
            }
        }
    }

    // Build the transform expression
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
                if let Some((_, field_value)) = partition_field_values.get(field_index) {
                    let field_value = Arc::new(field_value.clone());
                    let actual_insert_after = resolve_insert_after_with_change_type_redirection(
                        insert_after.clone(),
                        change_type_insert_after.clone(),
                    );
                    transform.with_inserted_field(actual_insert_after, field_value)
                } else {
                    return Err(crate::Error::InternalError(format!(
                        "Missing field value for partition column at index {field_index}"
                    )));
                }
            }
            Cdf {
                field_index,
                insert_after,
                col_type: _,
            } => {
                let actual_insert_after = resolve_insert_after_with_change_type_redirection(
                    insert_after.clone(),
                    change_type_insert_after.clone(),
                );

                if let Some((_, field_value)) = cdf_field_values.get(field_index) {
                    let field_value = Arc::new(field_value.clone());
                    transform.with_inserted_field(actual_insert_after, field_value)
                } else {
                    // CDF field was not generated, meaning it should come from physical data
                    // (e.g., ChangeType column in CDC files)
                    transform
                }
            }
        }
    }

    Ok(Arc::new(Expression::Transform(transform)))
}

/// Resolves the correct insert_after field for a column, applying change_type redirection when needed.
///
/// When a column wants to insert after "_change_type" but the change type column itself
/// needs to be redirected (e.g., from "_change_type" to "name"), this function handles
/// the redirection so the column goes to the correct location.
///
/// Example: CommitVersion wants to go after "_change_type", but "_change_type" goes after "name"
/// → CommitVersion gets redirected to go after "name" instead
fn resolve_insert_after_with_change_type_redirection(
    insert_after: Option<String>,
    change_type_insert_after: Option<String>,
) -> Option<String> {
    if insert_after
        .as_ref()
        .map(|field| field == CHANGE_TYPE_COL_NAME)
        .unwrap_or(false)
    {
        change_type_insert_after.or(insert_after)
    } else {
        insert_after
    }
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
            partition_values: HashMap::from([
                ("age".to_string(), "20".to_string()),
                ("score".to_string(), "100".to_string()),
            ]),
            commit_version: 42,
            commit_timestamp: 1234,
        }
    }

    fn create_test_logical_schema() -> StructType {
        StructType::new([
            StructField::nullable("id", DataType::STRING), // index 0
            StructField::not_null("age", DataType::LONG),  // index 1
            StructField::nullable("name", DataType::STRING), // index 2
            StructField::not_null("score", DataType::LONG), // index 3
            StructField::not_null(CHANGE_TYPE_COL_NAME, DataType::STRING), // index 4
            StructField::not_null(COMMIT_VERSION_COL_NAME, DataType::LONG), // index 5
            StructField::not_null(COMMIT_TIMESTAMP_COL_NAME, DataType::TIMESTAMP), // index 6
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
            ColumnType::Cdf(
                crate::scan::CdfCol::ChangeType(CHANGE_TYPE_COL_NAME.to_string()),
                4, // updated index for new schema
            ),
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
    fn test_column_ordering_with_change_type() {
        let logical_schema = create_test_logical_schema();
        let scan_file = create_test_scan_file(CdfScanFileType::Add);

        // Create a scenario: physical col A, partition col B, physical col C, partition col D, cdf col E
        // This tests that change_type_insert_after doesn't mess up the ordering of other columns
        let all_fields = vec![
            ColumnType::Selected("id".to_string()), // physical col A (index 0)
            ColumnType::Partition(1), // partition col B (age, index 1) - should insert after "id"
            ColumnType::Selected("name".to_string()), // physical col C (index 2)
            ColumnType::Partition(3), // partition col D (score, index 3) - should insert after "name"
            ColumnType::Cdf(
                crate::scan::CdfCol::ChangeType(CHANGE_TYPE_COL_NAME.to_string()),
                4, // CDF col E (index 4) - sets change_type_insert_after
            ),
            ColumnType::Cdf(
                crate::scan::CdfCol::CommitVersion,
                5, // CDF col F (index 5) - should NOT use change_type_insert_after for partition col D
            ),
        ];

        let transform_spec = Scan::get_transform_spec(&all_fields);

        // This should not panic or produce wrong ordering
        let result = get_cdf_transform_expr(
            &transform_spec,
            &scan_file.partition_values,
            &logical_schema,
            &scan_file,
        );

        // Extract the transform and check the actual column ordering
        let transform_expr = result.unwrap();
        if let crate::expressions::Expression::Transform(transform) = transform_expr.as_ref() {
            // CORRECT BEHAVIOR: Check that columns are inserted in their proper locations

            println!("transform: {:?}", transform);

            // The "age" partition column should be inserted after "id"
            let id_transform = transform.field_transforms.get("id");
            assert!(
                id_transform.is_some(),
                "Age partition should be inserted after 'id'"
            );
            let id_exprs = &id_transform.unwrap().exprs;
            assert_eq!(
                id_exprs.len(),
                1,
                "Should have exactly 1 expression after 'id' (age partition)"
            );

            // The "score" partition and "_change_type" CDF should be inserted after "name"
            let name_transform = transform.field_transforms.get("name");
            assert!(
                name_transform.is_some(),
                "Score partition and ChangeType CDF should be after 'name'"
            );
            let name_exprs = &name_transform.unwrap().exprs;
            assert_eq!(
                name_exprs.len(),
                3,
                "Should have exactly 3 expressions after 'name' (score partition + ChangeType CDF + CommitVersion CDF)"
            );

            // Verify no other unexpected transform locations
            assert_eq!(
                transform.field_transforms.len(),
                2,
                "Should have transforms for exactly 2 fields: id, name"
            );
        } else {
            panic!("Expected Transform expression");
        }
    }

    #[test]
    fn test_dynamic_metadata_physical_schema_check() {
        let logical_schema = create_test_logical_schema();

        // Test 1: Physical schema WITHOUT _change_type (Add/Remove files)

        let add_scan_file = create_test_scan_file(CdfScanFileType::Add);
        let all_fields = vec![
            ColumnType::Selected("id".to_string()),
            ColumnType::Cdf(
                crate::scan::CdfCol::ChangeType(CHANGE_TYPE_COL_NAME.to_string()),
                4, // updated index for new schema
            ),
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
