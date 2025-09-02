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

/// Extracts CDF expressions by field index from a scan file, similar to parse_partition_values
pub(crate) fn get_cdf_expressions_by_index(
    scan_file: &CdfScanFile,
    logical_schema: &StructType,
    transform_spec: &[FieldTransformSpec],
) -> DeltaResult<HashMap<usize, (String, Expression)>> {
    let mut result = HashMap::new();

    for field_transform in transform_spec {
        if let FieldTransformSpec::PartitionColumn { field_index, .. } = field_transform {
            let field = logical_schema.fields.get_index(*field_index);
            let Some((_, field)) = field else {
                continue;
            };
            let field_name = field.name();

            // Check if this is a CDF metadata column
            let expression = if field_name == CHANGE_TYPE_COL_NAME {
                let change_type = match scan_file.scan_type {
                    CdfScanFileType::Cdc => Expression::column([CHANGE_TYPE_COL_NAME]),
                    CdfScanFileType::Add => Expression::literal(ADD_CHANGE_TYPE),
                    CdfScanFileType::Remove => Expression::literal(REMOVE_CHANGE_TYPE),
                };
                Some(change_type)
            } else if field_name == COMMIT_VERSION_COL_NAME {
                Some(Expression::literal(scan_file.commit_version))
            } else if field_name == COMMIT_TIMESTAMP_COL_NAME {
                let timestamp = Scalar::timestamp_from_millis(scan_file.commit_timestamp)?;
                Some(timestamp.into())
            } else {
                None // Not a CDF column, skip
            };

            if let Some(expr) = expression {
                result.insert(*field_index, (field_name.to_string(), expr));
            }
        }
    }

    Ok(result)
}

/// Create the unified transform expression that handles both partition and CDF metadata columns
pub(crate) fn create_unified_transform_expr(
    scan_file: &CdfScanFile,
    logical_schema: &StructType,
    transform_spec: &[FieldTransformSpec],
) -> DeltaResult<ExpressionRef> {
    // Start with traditional partition values using the shared utility
    let mut field_values = crate::scan::parse_partition_values_to_expressions(
        logical_schema,
        transform_spec,
        &scan_file.partition_values,
    )?;

    // Add CDF metadata expressions
    let cdf_expressions = get_cdf_expressions_by_index(scan_file, logical_schema, transform_spec)?;
    field_values.extend(cdf_expressions);

    // Use the shared transform building utility
    crate::scan::build_transform_expr(transform_spec, field_values)
}
