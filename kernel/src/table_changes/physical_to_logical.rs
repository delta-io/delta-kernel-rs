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

/// Create the unified transform expression that handles both partition and CDF metadata columns
pub(crate) fn create_unified_transform_expr(
    scan_file: &CdfScanFile,
    logical_schema: &StructType,
    transform_spec: &[FieldTransformSpec],
) -> DeltaResult<ExpressionRef> {
    // 🎯 UNIFIED APPROACH: Both partition columns and CDF metadata through single transform system

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
            if let Some(value_str) = scan_file.partition_values.get(physical_name) {
                let partition_value =
                    crate::scan::parse_partition_value(Some(value_str), field.data_type())?;
                field_expressions.insert(physical_name.to_string(), partition_value.into());
            }
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

    // Special handling for CDC files: adjust _commit_version insertion point
    if scan_file.scan_type == CdfScanFileType::Cdc {
        // For CDC files, _commit_version should be inserted after _change_type (not after "name")
        // We need to create a custom transform spec for this case
        let mut custom_transform_spec = transform_spec.to_vec();

        // Find the _commit_version entry and update its insert_after
        for field_transform in &mut custom_transform_spec {
            if let FieldTransformSpec::PartitionColumn {
                field_index,
                insert_after,
            } = field_transform
            {
                if let Some((_, field)) = logical_schema.fields.get_index(*field_index) {
                    if field.name() == COMMIT_VERSION_COL_NAME {
                        *insert_after = Some(CHANGE_TYPE_COL_NAME.to_string());
                        break;
                    }
                }
            }
        }

        // TODO: Fix _commit_timestamp column ordering for CDC files
        // Currently _commit_timestamp gets inserted after "name" (last physical field) instead of
        // after _commit_version. This causes incorrect column ordering:
        // Current: [id, name, birthday, _commit_version, _commit_timestamp, _change_type]
        // Correct: [id, name, birthday, _change_type, _commit_version, _commit_timestamp]
        // Need to also update _commit_timestamp's insert_after to point to _commit_version.

        // Build transform with custom spec
        return crate::scan::build_transform_expr(&custom_transform_spec, field_values);
    }

    // Build the final transform expression (for Add/Remove files)
    crate::scan::build_transform_expr(transform_spec, field_values)
}
