use std::collections::HashMap;

use crate::expressions::{Expression, Scalar};
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::transforms::{get_transform_expr, parse_partition_values, TransformSpec};
use crate::DeltaResult;

use super::scan_file::{CdfScanFile, CdfScanFileType};
use super::{
    ADD_CHANGE_TYPE, CHANGE_TYPE_COL_NAME, COMMIT_TIMESTAMP_COL_NAME, COMMIT_VERSION_COL_NAME,
    REMOVE_CHANGE_TYPE,
};

/// Gets CDF metadata columns from the logical schema and scan file.
///
/// This function directly looks up CDF columns in the schema and generates their values
/// based on the scan file metadata.
pub(crate) fn get_cdf_columns(
    logical_schema: &SchemaRef,
    scan_file: &CdfScanFile,
) -> crate::DeltaResult<HashMap<usize, (String, Scalar)>> {
    let mut out = HashMap::new();

    // Handle _change_type
    let change_type_field = logical_schema.field_with_index(CHANGE_TYPE_COL_NAME);
    match (change_type_field, &scan_file.scan_type) {
        (Some((idx, field)), CdfScanFileType::Add) => {
            let val = (
                field.name().to_string(),
                Scalar::String(ADD_CHANGE_TYPE.to_string()),
            );
            out.insert(idx, val);
        }
        (Some((idx, field)), CdfScanFileType::Remove) => {
            let val = (
                field.name().to_string(),
                Scalar::String(REMOVE_CHANGE_TYPE.to_string()),
            );
            out.insert(idx, val);
        }
        (Some(_), CdfScanFileType::Cdc) | (None, _) => { /* Do nothing */ }
    }

    // Handle _commit_timestamp
    let timestamp_field = logical_schema.field_with_index(COMMIT_TIMESTAMP_COL_NAME);
    if let Some((idx, field)) = timestamp_field {
        let val = (
            field.name().to_string(),
            Scalar::timestamp_from_millis(scan_file.commit_timestamp)?,
        );
        out.insert(idx, val);
    }

    // Handle _commit_version
    let version_field = logical_schema.field_with_index(COMMIT_VERSION_COL_NAME);
    if let Some((idx, field)) = version_field {
        let val = (
            field.name().to_string(),
            Scalar::Long(scan_file.commit_version),
        );
        out.insert(idx, val);
    }

    Ok(out)
}

/// Gets the physical schema that will be used to read data in the `scan_file` path.
pub(crate) fn scan_file_physical_schema(
    scan_file: &CdfScanFile,
    physical_schema: &StructType,
) -> SchemaRef {
    if scan_file.scan_type == CdfScanFileType::Cdc {
        let change_type = StructField::not_null(CHANGE_TYPE_COL_NAME, DataType::STRING);
        let fields = physical_schema.fields().cloned().chain(Some(change_type));
        // NOTE: We don't validate the fields again because CHANGE_TYPE_COL_NAME should never be used anywhere else
        StructType::new_unchecked(fields).into()
    } else {
        physical_schema.clone().into()
    }
}

/// Prepares partition values for CDF columns and partition columns.
///
/// This function collects all partition values needed for the transform:
/// - Partition values from the scan file
/// - CDF metadata (_commit_version, _commit_timestamp)
/// - _change_type value (for Dynamic column when not physical)
pub(crate) fn prepare_cdf_partition_values(
    scan_file: &CdfScanFile,
    logical_schema: &SchemaRef,
    transform_spec: &TransformSpec,
) -> HashMap<usize, (String, Scalar)> {
    let mut partition_values = HashMap::new();

    // Handle regular partition values using parse_partition_values
    if let Ok(parsed_values) =
        parse_partition_values(logical_schema, transform_spec, &scan_file.partition_values)
    {
        partition_values.extend(parsed_values);
    }

    // Handle CDF metadata columns
    if let Ok(cdf_values) = get_cdf_columns(logical_schema, scan_file) {
        partition_values.extend(cdf_values);
    }

    partition_values
}

/// Generates the transform expression for converting physical data to logical data.
pub(crate) fn get_cdf_transform_expr(
    transform_spec: &TransformSpec,
    scan_file: &CdfScanFile,
    logical_schema: &SchemaRef,
    physical_schema: &SchemaRef,
) -> DeltaResult<std::sync::Arc<Expression>> {
    let partition_values = prepare_cdf_partition_values(scan_file, logical_schema, transform_spec);
    let physical_schema = scan_file_physical_schema(scan_file, physical_schema.as_ref());
    get_transform_expr(transform_spec, partition_values, physical_schema.as_ref())
}
