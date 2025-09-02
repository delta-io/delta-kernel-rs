use std::collections::HashMap;

use crate::expressions::{Expression, ExpressionRef, Scalar};
use crate::scan::FieldTransformSpec;
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::{DeltaResult, Error};

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
                return Err(Error::generic(
                    "logical schema did not contain expected field, can't transform data",
                ));
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
    // 🔥 TWO-STEP APPROACH: Clean separation of concerns
    // Print for debugging, but avoid requiring Debug on FieldTransformSpec.
    eprintln!(
        "🔍 DEBUG: logical_schema fields: {:?}",
        logical_schema
            .fields()
            .map(|f| f.name())
            .collect::<Vec<_>>()
    );
    eprintln!(
        "🔍 DEBUG: transform_spec details: {:?}",
        transform_spec
            .iter()
            .map(|spec| match spec {
                FieldTransformSpec::PartitionColumn {
                    field_index,
                    insert_after,
                } => Some(format!(
                    "PartitionColumn(field_index: {}, insert_after: {:?})",
                    field_index, insert_after
                )),
                _ => Some("Other".to_string()),
            })
            .collect::<Vec<_>>()
    );
    eprintln!(
        "🔍 DEBUG: scan_file.partition_values: {:?}",
        scan_file.partition_values
    );

    // Step 1: Handle regular partition columns (automatically skips CDF metadata)
    let mut field_values = crate::scan::parse_partition_values_to_expressions(
        logical_schema,
        transform_spec,
        &scan_file.partition_values, // Only contains real partition columns
    )?;

    eprintln!("🔍 DEBUG: field_values: {:?}", field_values);

    // Step 2: Add back only the requested CDF metadata columns
    let cdf_expressions = get_cdf_expressions_by_index(scan_file, logical_schema, transform_spec)?;
    eprintln!("🔍 DEBUG: cdf_expressions: {:?}", cdf_expressions);
    field_values.extend(cdf_expressions);

    // Build the final transform expression
    let result = crate::scan::build_transform_expr(transform_spec, field_values)?;
    eprintln!("🔍 DEBUG: result: {:?}", result);
    eprintln!("--------------------------------");

    Ok(result)
}
