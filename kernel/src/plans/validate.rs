//! Validation for declarative plan nodes.

use crate::expressions::{ColumnName, Scalar};
use crate::plans::ir::ScanFile;
use crate::schema::{SchemaRef, StructField, StructType};
use crate::{DeltaResult, Error};

/// Validates per-file constants for a scan node.
///
/// # Errors
///
/// [`Error::Schema`] when invariants on `files`, `file_constant_columns`, or `schema` are violated.
pub(crate) fn validate_scan_files(
    files: &[ScanFile],
    file_constant_columns: &[ColumnName],
    schema: &StructType,
) -> DeltaResult<()> {
    let column_count = file_constant_columns.len();
    for (file_idx, file) in files.iter().enumerate() {
        if file.file_constants.len() != column_count {
            return Err(Error::schema(format!(
                "Scan file {file_idx} has {} file-constant value(s), expected {column_count}",
                file.file_constants.len()
            )));
        }
    }

    let mut seen = std::collections::HashSet::new();
    for (col_idx, col) in file_constant_columns.iter().enumerate() {
        if !seen.insert(col) {
            return Err(Error::schema(format!(
                "Duplicate file-constant column: {col}"
            )));
        }

        let fields = schema.walk_column_fields(col)?;
        let Some(leaf) = fields.last() else {
            return Err(Error::generic(format!(
                "File-constant column '{col}' has an empty path"
            )));
        };
        if leaf.is_metadata_column() {
            return Err(Error::schema(format!(
                "File-constant column '{col}' is a metadata column; metadata columns are \
                 engine-generated and cannot be listed in file_constant_columns"
            )));
        }

        for (file_idx, file) in files.iter().enumerate() {
            validate_scalar_for_field(&file.file_constants[col_idx], leaf, col).map_err(|e| {
                Error::schema(format!(
                    "Scan file {file_idx}, file-constant column '{col}': {e}"
                ))
            })?;
        }
    }

    Ok(())
}

fn validate_scalar_for_field(
    scalar: &Scalar,
    field: &StructField,
    col: &ColumnName,
) -> DeltaResult<()> {
    if scalar.is_null() {
        if !field.nullable {
            return Err(Error::schema(format!(
                "File-constant column '{col}' is non-nullable but scalar is null"
            )));
        }
        return Ok(());
    }

    if scalar.data_type() != *field.data_type() {
        return Err(Error::schema(format!(
            "File-constant column '{col}' has type {} but scalar has type {}",
            field.data_type(),
            scalar.data_type()
        )));
    }
    Ok(())
}

/// Validates scan files against a [`SchemaRef`].
pub(crate) fn validate_scan_files_schema(
    files: &[ScanFile],
    file_constant_columns: &[ColumnName],
    schema: SchemaRef,
) -> DeltaResult<()> {
    validate_scan_files(files, file_constant_columns, schema.as_ref())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::expressions::column_name;
    use crate::schema::{DataType, MetadataColumnSpec, StructField, StructType};
    use crate::FileMeta;

    fn test_file() -> ScanFile {
        ScanFile::new(FileMeta {
            location: url::Url::parse("file:///a.parquet").unwrap(),
            last_modified: 0,
            size: 0,
        })
    }

    fn schema_with_part() -> SchemaRef {
        Arc::new(StructType::new_unchecked([
            StructField::not_null("id", DataType::LONG),
            StructField::not_null("part", DataType::STRING),
        ]))
    }

    #[test]
    fn validate_scan_files_accepts_matching_constants() {
        let files = vec![ScanFile {
            meta: test_file().meta,
            file_constants: vec![Scalar::String("east".into())],
        }];
        validate_scan_files_schema(
            &files,
            &[column_name!("part")],
            schema_with_part(),
        )
        .unwrap();
    }

    #[test]
    fn validate_scan_files_rejects_length_mismatch() {
        let files = vec![test_file()];
        let err = validate_scan_files_schema(&files, &[column_name!("part")], schema_with_part())
            .unwrap_err();
        assert!(err.to_string().contains("expected 1"));
    }

    #[test]
    fn validate_scan_files_rejects_metadata_column() {
        let schema = Arc::new(StructType::new_unchecked([
            StructField::create_metadata_column("row_index", MetadataColumnSpec::RowIndex),
        ]));
        let files = vec![ScanFile {
            meta: test_file().meta,
            file_constants: vec![Scalar::Long(0)],
        }];
        let err =
            validate_scan_files_schema(&files, &[column_name!("row_index")], schema).unwrap_err();
        assert!(err.to_string().contains("metadata column"));
    }
}
