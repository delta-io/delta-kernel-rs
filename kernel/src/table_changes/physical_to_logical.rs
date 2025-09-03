use crate::expressions::{Expression, ExpressionRef, Scalar};
use crate::scan::FieldTransformSpec;
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::DeltaResult;
use std::collections::HashSet;

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
        // CDC files have _change_type in physical data, add it to physical schema
        let change_type = StructField::not_null(CHANGE_TYPE_COL_NAME, DataType::STRING);
        let fields = physical_schema.fields().cloned().chain(Some(change_type));
        StructType::new(fields).into()
    } else {
        // Add/Remove files have no CDF metadata in physical data
        // CDF columns will be added via StaticReplace transforms
        physical_schema.clone().into()
    }
}

/// Create the unified transform expression that handles both partition and CDF metadata columns
pub(crate) fn create_unified_transform_expr(
    scan_file: &CdfScanFile,
    logical_schema: &StructType,
    transform_spec: &[FieldTransformSpec],
) -> DeltaResult<ExpressionRef> {
    // Step 1: Parse partition values using existing utility
    let mut field_values = crate::scan::parse_partition_values_to_expressions(
        logical_schema,
        transform_spec,
        &scan_file.partition_values,
    )?;

    // Step 2: Create CDF metadata expressions only for fields present in logical schema
    let cdf_expressions = create_cdf_field_expressions(scan_file, logical_schema)?;

    // Step 3: Add CDF expressions to field_values, using unified utility
    let cdf_field_values = crate::scan::parse_field_values_to_expressions(
        logical_schema,
        transform_spec,
        &cdf_expressions,
    )?;

    // Merge CDF field values with partition field values
    field_values.extend(cdf_field_values);

    // Step 4: Use shared utility to build the complete transform
    crate::scan::build_transform_expr(transform_spec, field_values)
}

/// Create CDF metadata expressions for fields that exist in the logical schema
fn create_cdf_field_expressions(
    scan_file: &CdfScanFile,
    logical_schema: &StructType,
) -> DeltaResult<std::collections::HashMap<String, Expression>> {
    use std::collections::HashMap;

    let mut cdf_expressions = HashMap::new();

    // Only create expressions for CDF fields that exist in the logical schema
    let logical_field_names: HashSet<&str> = logical_schema
        .fields()
        .map(|field| field.name().as_str())
        .collect();

    // Add _change_type if present in logical schema
    if logical_field_names.contains(CHANGE_TYPE_COL_NAME) {
        let change_type_expr = match scan_file.scan_type {
            CdfScanFileType::Add => Expression::literal(ADD_CHANGE_TYPE),
            CdfScanFileType::Remove => Expression::literal(REMOVE_CHANGE_TYPE),
            CdfScanFileType::Cdc => {
                // For CDC files, _change_type exists physically - no need to add expression
                // It will be handled by the normal column mapping
                return Ok(cdf_expressions);
            }
        };
        cdf_expressions.insert(CHANGE_TYPE_COL_NAME.to_string(), change_type_expr);
    }

    // Add _commit_version if present in logical schema
    if logical_field_names.contains(COMMIT_VERSION_COL_NAME) {
        cdf_expressions.insert(
            COMMIT_VERSION_COL_NAME.to_string(),
            Expression::literal(scan_file.commit_version),
        );
    }

    // Add _commit_timestamp if present in logical schema
    if logical_field_names.contains(COMMIT_TIMESTAMP_COL_NAME) {
        if let Ok(timestamp) = Scalar::timestamp_from_millis(scan_file.commit_timestamp) {
            cdf_expressions.insert(
                COMMIT_TIMESTAMP_COL_NAME.to_string(),
                Expression::literal(timestamp),
            );
        }
    }

    Ok(cdf_expressions)
}
