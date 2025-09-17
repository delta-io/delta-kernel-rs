use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools;

use crate::expressions::Scalar;
use crate::scan::{parse_partition_value, ColumnType, FieldTransformSpec, TransformSpec};
use crate::schema::{ColumnName, DataType, Schema, SchemaRef, StructField, StructType};
use crate::{DeltaResult, Error, Expression};

use super::scan_file::{CdfScanFile, CdfScanFileType};
use super::{
    ADD_CHANGE_TYPE, CHANGE_TYPE_COL_NAME, COMMIT_TIMESTAMP_COL_NAME, COMMIT_VERSION_COL_NAME,
    REMOVE_CHANGE_TYPE,
};

/// Returns a map from change data feed column name to an expression that generates the row data.
pub fn get_cdf_columns(
    logical_schema: &Schema,
    scan_file: &CdfScanFile,
) -> DeltaResult<HashMap<usize, (String, Scalar)>> {
    let mut out = HashMap::new();
    let change_type_field = logical_schema.fields.get_full(CHANGE_TYPE_COL_NAME);
    match (change_type_field, &scan_file.scan_type) {
        (Some(field_info), CdfScanFileType::Add) => {
            let val = (
                field_info.1.to_string(),
                Scalar::String(ADD_CHANGE_TYPE.to_string()),
            );
            out.insert(field_info.0, val);
        }
        (Some(field_info), CdfScanFileType::Remove) => {
            let val = (
                field_info.1.to_string(),
                Scalar::String(REMOVE_CHANGE_TYPE.to_string()),
            );
            out.insert(field_info.0, val);
        }
        (Some(_), CdfScanFileType::Cdc) | (None, _) => { /* Do nothing */ }
    }

    let timestamp_field = logical_schema.fields.get_full(COMMIT_TIMESTAMP_COL_NAME);
    if let Some(field_info) = timestamp_field {
        let val = (
            field_info.1.to_string(),
            Scalar::timestamp_from_millis(scan_file.commit_timestamp)?,
        );
        out.insert(field_info.0, val);
    }

    let version_field = logical_schema.fields.get_full(COMMIT_VERSION_COL_NAME);
    if let Some(field_info) = version_field {
        let val = (
            field_info.1.to_string(),
            Scalar::Long(scan_file.commit_version),
        );
        out.insert(field_info.0, val);
    }

    Ok(out)
}

/// Generates the expression used to convert physical data from the `scan_file` path into logical
/// data matching the `logical_schema`
pub(crate) fn physical_to_logical_expr(
    scan_file: &CdfScanFile,
    logical_schema: &StructType,
    physical_schema: &StructType,
    all_fields: &[ColumnType],
) -> DeltaResult<Expression> {
    todo!()
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
