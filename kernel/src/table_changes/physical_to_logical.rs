use std::collections::HashMap;

use crate::expressions::{Expression, ExpressionRef, Scalar};
use crate::scan::FieldTransformSpec;
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::DeltaResult;

use super::scan_file::{CdfScanFile, CdfScanFileType};
use super::{
    ADD_CHANGE_TYPE, CHANGE_TYPE_COL_NAME, COMMIT_TIMESTAMP_COL_NAME, COMMIT_VERSION_COL_NAME,
    REMOVE_CHANGE_TYPE,
};

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

/// Create CDF metadata expressions map for the unified transform approach
pub(crate) fn get_cdf_metadata_expressions(scan_file: &CdfScanFile) -> HashMap<String, Expression> {
    let mut cdf_expressions = HashMap::new();

    // Add _change_type expression only for Add/Remove files
    // For CDC files, _change_type already exists in physical data, so skip it
    match scan_file.scan_type {
        CdfScanFileType::Cdc => {
            // Skip _change_type - it already exists in physical schema
        }
        CdfScanFileType::Add => {
            cdf_expressions.insert(
                CHANGE_TYPE_COL_NAME.to_string(),
                Expression::literal(ADD_CHANGE_TYPE),
            );
        }
        CdfScanFileType::Remove => {
            cdf_expressions.insert(
                CHANGE_TYPE_COL_NAME.to_string(),
                Expression::literal(REMOVE_CHANGE_TYPE),
            );
        }
    }

    // Add _commit_version expression
    cdf_expressions.insert(
        COMMIT_VERSION_COL_NAME.to_string(),
        Expression::literal(scan_file.commit_version),
    );

    // Add _commit_timestamp expression
    if let Ok(timestamp) = Scalar::timestamp_from_millis(scan_file.commit_timestamp) {
        cdf_expressions.insert(
            COMMIT_TIMESTAMP_COL_NAME.to_string(),
            Expression::literal(timestamp),
        );
    }

    cdf_expressions
}

/// Create a unified transform expression for CDF that handles both partition columns and CDF metadata.
///
/// This function replaces the old approach of building struct expressions manually by using the
/// shared transform system. It combines:
/// - Partition values (as literal expressions) 
/// - CDF metadata (_change_type, _commit_version, _commit_timestamp)
/// 
/// The result is an Expression::Transform that can convert physical data to logical CDF format.
pub(crate) fn create_unified_transform_expr(
    scan_file: &CdfScanFile,
    logical_schema: &StructType,
    transform_spec: &[FieldTransformSpec],
) -> DeltaResult<ExpressionRef> {
    // Unified approach: Both partition columns and CDF metadata through single transform system

    // Create unified map of all field expressions (partition + CDF metadata)
    let mut field_expressions = HashMap::new();

    // Add partition values as expressions
    for field_transform in transform_spec {
        if let FieldTransformSpec::PartitionColumn { field_index, .. } = field_transform {
            let field = logical_schema.fields.get_index(*field_index);
            let Some((_, field)) = field else {
                continue;
            };

            let physical_name = field.physical_name();
            let partition_value = crate::scan::parse_partition_value(
                scan_file.partition_values.get(physical_name), 
                field.data_type()
            )?;
            field_expressions.insert(physical_name.to_string(), partition_value.into());
        }
    }

    // Add CDF metadata expressions (these will override partition values if there's a name conflict)
    let cdf_expressions = get_cdf_metadata_expressions(scan_file);
    field_expressions.extend(cdf_expressions);

    // Parse field expressions using the unified function
    let field_values = crate::scan::parse_field_values_to_expressions(
        logical_schema,
        transform_spec,
        &field_expressions,
    )?;

    // Build the final transform expression (for Add/Remove files)
    crate::scan::build_transform_expr(transform_spec, field_values)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::expressions::{Expression as Expr, Scalar};
    use crate::scan::{ColumnType, Scan};
    use crate::schema::{DataType, StructField, StructType};
    use crate::table_changes::physical_to_logical::{
        create_unified_transform_expr, get_cdf_metadata_expressions,
    };
    use crate::table_changes::scan_file::{CdfScanFile, CdfScanFileType};
    use crate::table_changes::{
        ADD_CHANGE_TYPE, CHANGE_TYPE_COL_NAME, COMMIT_TIMESTAMP_COL_NAME, COMMIT_VERSION_COL_NAME,
        REMOVE_CHANGE_TYPE,
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
    fn test_cdf_metadata_expressions() {
        // Test Add file
        let add_scan_file = create_test_scan_file(CdfScanFileType::Add);
        let add_expressions = get_cdf_metadata_expressions(&add_scan_file);
        assert_eq!(add_expressions.len(), 3);
        assert_eq!(add_expressions[CHANGE_TYPE_COL_NAME], Expr::literal(ADD_CHANGE_TYPE));
        assert_eq!(add_expressions[COMMIT_VERSION_COL_NAME], Expr::literal(42i64));
        assert_eq!(add_expressions[COMMIT_TIMESTAMP_COL_NAME], Expr::literal(Scalar::Timestamp(1234000)));

        // Test Remove file
        let remove_scan_file = create_test_scan_file(CdfScanFileType::Remove);
        let remove_expressions = get_cdf_metadata_expressions(&remove_scan_file);
        assert_eq!(remove_expressions.len(), 3);
        assert_eq!(remove_expressions[CHANGE_TYPE_COL_NAME], Expr::literal(REMOVE_CHANGE_TYPE));

        // Test CDC file (should not have _change_type)
        let cdc_scan_file = create_test_scan_file(CdfScanFileType::Cdc);
        let cdc_expressions = get_cdf_metadata_expressions(&cdc_scan_file);
        assert_eq!(cdc_expressions.len(), 2);
        assert!(!cdc_expressions.contains_key(CHANGE_TYPE_COL_NAME));
    }

    #[test]
    fn test_create_unified_transform_expr() {
        let logical_schema = create_test_logical_schema();
        
        // Test with partition columns
        let scan_file = create_test_scan_file(CdfScanFileType::Add);
        let all_fields = vec![
            ColumnType::Selected("id".to_string()),
            ColumnType::Partition(1), // age column
            ColumnType::Metadata {
                physical_name: CHANGE_TYPE_COL_NAME.to_string(),
                logical_idx: 2,
                use_as_selected: false,
            },
        ];
        let transform_spec = Scan::get_transform_spec(&all_fields);
        let result = create_unified_transform_expr(&scan_file, &logical_schema, &transform_spec);
        assert!(result.is_ok());
        assert!(matches!(result.unwrap().as_ref(), Expr::Transform(_)));

        // Test with null partition (the critical bug fix)
        let mut null_scan_file = create_test_scan_file(CdfScanFileType::Add);
        null_scan_file.partition_values.clear();
        let result = create_unified_transform_expr(&null_scan_file, &logical_schema, &transform_spec);
        assert!(result.is_ok());
        assert!(matches!(result.unwrap().as_ref(), Expr::Transform(_)));
    }
}
