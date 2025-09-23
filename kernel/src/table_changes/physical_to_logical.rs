use std::collections::HashMap;

use crate::expressions::Scalar;
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::transforms::{parse_partition_values, TransformSpec};
use crate::{DeltaResult, Error};

use super::scan_file::{CdfScanFile, CdfScanFileType};
use super::{CHANGE_TYPE_COL_NAME, COMMIT_TIMESTAMP_COL_NAME, COMMIT_VERSION_COL_NAME};

/// Gets CDF metadata columns from the logical schema and scan file.
///
/// This function directly looks up CDF columns in the schema and generates their values
/// based on the scan file metadata, returning an iterator over the metadata.
fn get_cdf_columns(
    logical_schema: &SchemaRef,
    scan_file: &CdfScanFile,
) -> DeltaResult<impl Iterator<Item = (usize, (String, Scalar))>> {
    // Handle _change_type
    let change_type_field = logical_schema.field_with_index(CHANGE_TYPE_COL_NAME);
    let change_type_metadata = match (change_type_field, &scan_file.scan_type) {
        (Some((idx, field)), CdfScanFileType::Add | CdfScanFileType::Remove) => {
            let name = field.name().to_string();
            let value = Scalar::String(scan_file.scan_type.get_cdf_string_value().to_string());
            Some((idx, (name, value)))
        }
        (Some(_), CdfScanFileType::Cdc) | (None, _) => {
            // Cdc files contain the `change_type_` column physically, so we do not insert a metadata-derived value
            None
        }
    };

    // Handle _commit_timestamp
    let timestamp_field = logical_schema.field_with_index(COMMIT_TIMESTAMP_COL_NAME);
    let timestamp_metadata: Result<Option<(usize, (String, Scalar))>, Error> = timestamp_field
        .map(|(idx, field)| {
            let value = Scalar::timestamp_from_millis(scan_file.commit_timestamp).map_err(|e| {
                Error::generic(format!("Failed to process {}: {e}", scan_file.path))
            })?;
            Ok((idx, (field.name().to_string(), value)))
        })
        .transpose();

    // Handle _commit_version
    let version_field = logical_schema.field_with_index(COMMIT_VERSION_COL_NAME);
    let version_metadata = version_field.map(|(idx, field)| {
        let name = field.name().to_string();
        let value = Scalar::Long(scan_file.commit_version);
        (idx, (name, value))
    });

    Ok(change_type_metadata
        .into_iter()
        .chain(timestamp_metadata?)
        .chain(version_metadata))
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
) -> DeltaResult<HashMap<usize, (String, Scalar)>> {
    let mut partition_values = HashMap::new();

    // Handle regular partition values using parse_partition_values
    let parsed_values =
        parse_partition_values(logical_schema, transform_spec, &scan_file.partition_values)?;
    partition_values.extend(parsed_values);

    // Handle CDF metadata columns
    let cdf_values = get_cdf_columns(logical_schema, scan_file)?;
    partition_values.extend(cdf_values);

    Ok(partition_values)
}
