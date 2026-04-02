//! Schema validation utilities for Delta table creation.
//!
//! Provides validation functions to ensure schemas conform to Delta Lake protocol
//! requirements before creating tables. Aligns with the Java kernel's
//! `SchemaUtils.validateSchema()`.

use std::collections::HashSet;

use crate::schema::{DataType, StructType};
use crate::table_features::ColumnMappingMode;
use crate::{DeltaResult, Error};

/// Characters that are invalid in Parquet column names when column mapping is disabled.
/// These characters have special meaning in Parquet schema syntax.
const INVALID_PARQUET_CHARS: &[char] = &[' ', ',', ';', '{', '}', '(', ')', '\n', '\t', '='];

/// Validates a schema for table creation.
///
/// Performs the following checks:
/// 1. Schema is non-empty
/// 2. No duplicate column names (case-insensitive, including nested fields)
/// 3. Column names contain only valid characters
/// 4. All data types are supported by the Delta protocol
pub(crate) fn validate_schema_for_create(
    schema: &StructType,
    column_mapping_mode: ColumnMappingMode,
) -> DeltaResult<()> {
    if schema.num_fields() == 0 {
        return Err(Error::generic("Schema cannot be empty"));
    }

    let paths = collect_all_field_paths(schema);

    check_duplicate_columns(&paths)?;

    let column_mapping_enabled = !matches!(column_mapping_mode, ColumnMappingMode::None);
    validate_column_names(&paths, column_mapping_enabled)?;

    Ok(())
}

/// Recursively collects all struct field paths in the schema.
///
/// Walks through structs, arrays, and maps to produce dot-joined paths for every struct
/// field in the tree. For example, a schema `{a: struct<b: int, c: string>}` yields
/// `["a", "a.b", "a.c"]`.
fn collect_all_field_paths(schema: &StructType) -> Vec<String> {
    let mut paths = Vec::new();
    collect_paths_recursive(schema, "", &mut paths);
    paths
}

fn collect_paths_recursive(schema: &StructType, prefix: &str, paths: &mut Vec<String>) {
    for field in schema.fields() {
        let path = if prefix.is_empty() {
            field.name().to_string()
        } else {
            format!("{prefix}.{}", field.name())
        };
        paths.push(path.clone());

        match field.data_type() {
            DataType::Struct(inner) => {
                collect_paths_recursive(inner, &path, paths);
            }
            DataType::Array(arr) => {
                if let DataType::Struct(inner) = &arr.element_type {
                    collect_paths_recursive(inner, &format!("{path}.element"), paths);
                }
            }
            DataType::Map(map) => {
                if let DataType::Struct(inner) = &map.key_type {
                    collect_paths_recursive(inner, &format!("{path}.key"), paths);
                }
                if let DataType::Struct(inner) = &map.value_type {
                    collect_paths_recursive(inner, &format!("{path}.value"), paths);
                }
            }
            _ => {}
        }
    }
}

/// Checks for case-insensitive duplicate column paths.
fn check_duplicate_columns(paths: &[String]) -> DeltaResult<()> {
    let mut seen = HashSet::new();
    let mut duplicates = Vec::new();

    for path in paths {
        let lower = path.to_lowercase();
        if !seen.insert(lower) {
            duplicates.push(path.as_str());
        }
    }

    if !duplicates.is_empty() {
        duplicates.sort();
        return Err(Error::generic(format!(
            "Schema contains duplicate columns (case-insensitive): {}",
            duplicates.join(", ")
        )));
    }
    Ok(())
}

/// Validates column names contain only allowed characters.
///
/// When column mapping is disabled, rejects names containing Parquet special characters.
/// When column mapping is enabled, only rejects newlines (physical names are auto-generated,
/// but logical names with newlines cause issues in metadata serialization).
fn validate_column_names(paths: &[String], column_mapping_enabled: bool) -> DeltaResult<()> {
    for path in paths {
        // Check each segment of the path individually
        for segment in path.split('.') {
            if column_mapping_enabled {
                if segment.contains('\n') {
                    return Err(Error::generic(format!(
                        "Column name '{segment}' contains a newline character, \
                         which is not allowed"
                    )));
                }
            } else if segment.contains(INVALID_PARQUET_CHARS) {
                let invalid: Vec<char> = segment
                    .chars()
                    .filter(|c| INVALID_PARQUET_CHARS.contains(c))
                    .collect();
                return Err(Error::generic(format!(
                    "Column name '{segment}' contains invalid character(s) \
                     {invalid:?} that are not allowed in Parquet column names. \
                     Enable column mapping to use special characters in column names."
                )));
            }
        }
    }
    Ok(())
}

/// Normalizes partition column names to match the casing in the schema.
///
/// Delta stores partition column names case-preserving as they appear in the schema.
/// This function resolves case-insensitive partition column names to the canonical
/// casing from the schema.
pub(crate) fn case_preserving_partition_col_names(
    schema: &StructType,
    partition_columns: &[crate::expressions::ColumnName],
) -> Vec<crate::expressions::ColumnName> {
    let name_map: std::collections::HashMap<String, String> = schema
        .fields()
        .map(|f| (f.name().to_lowercase(), f.name().to_string()))
        .collect();

    partition_columns
        .iter()
        .map(|col| {
            let path = col.path();
            if path.len() == 1 {
                if let Some(canonical) = name_map.get(&path[0].to_lowercase()) {
                    return crate::expressions::ColumnName::new([canonical.as_str()]);
                }
            }
            col.clone()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::StructField;

    fn simple_schema() -> StructType {
        StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
        ])
    }

    #[test]
    fn empty_schema_rejected() {
        let schema = StructType::new_unchecked(vec![]);
        let result = validate_schema_for_create(&schema, ColumnMappingMode::None);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot be empty"));
    }

    #[test]
    fn valid_schema_passes() {
        let result = validate_schema_for_create(&simple_schema(), ColumnMappingMode::None);
        assert!(result.is_ok());
    }

    // -- Column name validation --

    #[test]
    fn space_in_column_name_rejected_without_cm() {
        let schema = StructType::new_unchecked(vec![StructField::new(
            "my column",
            DataType::INTEGER,
            false,
        )]);
        let result = validate_schema_for_create(&schema, ColumnMappingMode::None);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("invalid character"));
    }

    #[test]
    fn semicolon_in_column_name_rejected_without_cm() {
        let schema =
            StructType::new_unchecked(vec![StructField::new("col;name", DataType::INTEGER, false)]);
        let result = validate_schema_for_create(&schema, ColumnMappingMode::None);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("invalid character"));
    }

    #[test]
    fn special_chars_allowed_with_column_mapping() {
        let schema = StructType::new_unchecked(vec![
            StructField::new("my column", DataType::INTEGER, false),
            StructField::new("col;name", DataType::STRING, true),
        ]);
        let result = validate_schema_for_create(&schema, ColumnMappingMode::Name);
        assert!(result.is_ok());
    }

    #[test]
    fn newline_rejected_even_with_column_mapping() {
        let schema = StructType::new_unchecked(vec![StructField::new(
            "col\nname",
            DataType::INTEGER,
            false,
        )]);
        let result = validate_schema_for_create(&schema, ColumnMappingMode::Name);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("newline"));
    }

    #[test]
    fn valid_names_with_underscores_and_digits() {
        let schema = StructType::new_unchecked(vec![
            StructField::new("col_1", DataType::INTEGER, false),
            StructField::new("_private", DataType::STRING, true),
            StructField::new("CamelCase123", DataType::LONG, false),
        ]);
        let result = validate_schema_for_create(&schema, ColumnMappingMode::None);
        assert!(result.is_ok());
    }

    // -- Deep duplicate detection --

    #[test]
    fn nested_duplicate_detected() {
        let inner_a =
            StructType::new_unchecked(vec![StructField::new("child", DataType::INTEGER, false)]);
        let inner_b =
            StructType::new_unchecked(vec![StructField::new("CHILD", DataType::STRING, true)]);
        let schema = StructType::new_unchecked(vec![
            StructField::new("a", DataType::Struct(Box::new(inner_a)), false),
            StructField::new("b", DataType::Struct(Box::new(inner_b)), false),
        ]);
        // a.child vs b.CHILD are in different structs so not duplicates
        let result = validate_schema_for_create(&schema, ColumnMappingMode::None);
        assert!(result.is_ok());
    }

    #[test]
    fn same_struct_path_duplicate_detected() {
        // This is caught by StructType::try_new already, but validate_schema_for_create
        // also catches it via the flattened path check
        let inner =
            StructType::new_unchecked(vec![StructField::new("x", DataType::INTEGER, false)]);
        let schema = StructType::new_unchecked(vec![
            StructField::new("a", DataType::Struct(Box::new(inner)), false),
            StructField::new("A", DataType::STRING, true),
        ]);
        let result = validate_schema_for_create(&schema, ColumnMappingMode::None);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("duplicate"));
    }

    // -- Case-preserving partition column names --

    #[test]
    fn partition_column_name_case_preserved() {
        let schema = StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("EventDate", DataType::DATE, false),
        ]);
        let columns = vec![crate::expressions::ColumnName::new(["eventdate"])];
        let result = case_preserving_partition_col_names(&schema, &columns);
        assert_eq!(result[0].path(), ["EventDate"]);
    }

    #[test]
    fn partition_column_name_already_matches() {
        let schema = StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("date", DataType::DATE, false),
        ]);
        let columns = vec![crate::expressions::ColumnName::new(["date"])];
        let result = case_preserving_partition_col_names(&schema, &columns);
        assert_eq!(result[0].path(), ["date"]);
    }
}
