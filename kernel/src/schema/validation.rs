//! Schema validation utilities for Delta tables.
//!
//! This module provides validation functions to ensure schemas conform to Delta Lake
//! protocol requirements before creating or modifying tables.

use std::collections::HashSet;

use crate::schema::{DataType, StructType};
use crate::table_features::ColumnMappingMode;
use crate::{DeltaResult, Error};

// =============================================================================
// Constants
// =============================================================================

/// Characters that are invalid in Parquet column names when column mapping is disabled.
/// These characters have special meaning in Parquet schema syntax.
const INVALID_PARQUET_CHARS: &[char] = &[' ', ',', ';', '{', '}', '(', ')', '\n', '\t', '='];

/// Characters that are always invalid in column names, even with column mapping enabled.
const ALWAYS_INVALID_CHARS: &[char] = &['\n'];

// =============================================================================
// Public API
// =============================================================================

/// Validates a schema for table creation.
///
/// This performs the following validations:
/// 1. Checks for duplicate column names (case-insensitive, including nested fields)
/// 2. Validates column names contain only valid characters for Parquet
///
/// # Arguments
/// * `schema` - The schema to validate
/// * `column_mapping_mode` - The column mapping mode for the table
///
/// # Errors
/// Returns an error if:
/// - Duplicate column names are found (case-insensitive)
/// - Column names contain invalid characters
pub(crate) fn validate_schema_for_create(
    schema: &StructType,
    column_mapping_mode: ColumnMappingMode,
) -> DeltaResult<()> {
    // Collect all field paths (including nested)
    let paths = collect_all_field_paths(schema);

    // Check for case-insensitive duplicates
    check_duplicate_columns(&paths)?;

    // Validate column names based on column mapping mode
    let column_mapping_enabled = !matches!(column_mapping_mode, ColumnMappingMode::None);
    validate_column_names(schema, column_mapping_enabled)?;

    Ok(())
}

/// Validates partition columns against the schema.
///
/// This performs the following validations:
/// 1. Each partition column exists in the schema
/// 2. Partition columns are top-level fields (not nested)
/// 3. Partition columns have valid data types (primitives only)
///
/// # Arguments
/// * `schema` - The table schema
/// * `partition_columns` - The partition column names
///
/// # Errors
/// Returns an error if:
/// - A partition column doesn't exist in the schema
/// - A partition column has an unsupported data type (Struct, Array, Map, Variant)
pub(crate) fn validate_partition_columns(
    schema: &StructType,
    partition_columns: &[String],
) -> DeltaResult<()> {
    for col_name in partition_columns {
        // Find the column in the schema (case-insensitive)
        let field = schema
            .fields()
            .find(|f| f.name().eq_ignore_ascii_case(col_name))
            .ok_or_else(|| {
                Error::generic(format!(
                    "Partition column '{}' not found in schema",
                    col_name
                ))
            })?;

        // Check that the data type is valid for partitioning
        if !is_valid_partition_column_type(field.data_type()) {
            return Err(Error::generic(format!(
                "Partition column '{}' has unsupported type '{}'. \
                 Only primitive types (string, integer, long, boolean, date, timestamp, etc.) \
                 are allowed as partition columns.",
                col_name,
                field.data_type()
            )));
        }
    }

    Ok(())
}

// =============================================================================
// Private helpers
// =============================================================================

/// Checks if a data type is valid for partition columns.
///
/// Partition columns must be primitive types. Complex types (Struct, Array, Map, Variant)
/// are not allowed as partition columns.
fn is_valid_partition_column_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Primitive(_))
}

/// Collects all field paths from a schema, including nested struct fields.
///
/// For a schema like `{a: INT, b: {c: STRING, d: INT}}`, this returns:
/// `["a", "b", "b.c", "b.d"]`
///
/// For nested arrays/maps containing structs, paths include the fixed names:
/// - `items: array<struct<x, y>>` → `["items", "items.element.x", "items.element.y"]`
/// - `data: map<string, struct<x>>` → `["data", "data.value.x"]`
fn collect_all_field_paths(schema: &StructType) -> Vec<String> {
    let mut paths = Vec::new();
    collect_paths_recursive(schema, "", &mut paths);
    paths
}

/// Recursively collects field paths from a struct type.
fn collect_paths_recursive(struct_type: &StructType, prefix: &str, paths: &mut Vec<String>) {
    for field in struct_type.fields() {
        let path = if prefix.is_empty() {
            field.name().to_string()
        } else {
            format!("{}.{}", prefix, field.name())
        };

        paths.push(path.clone());

        // Recurse into nested types that may contain struct fields
        collect_paths_from_data_type(field.data_type(), &path, paths);
    }
}

/// Recursively collects field paths from nested data types.
///
/// This handles structs nested inside arrays and maps, where the fixed names
/// "element", "key", and "value" are used as path components.
fn collect_paths_from_data_type(data_type: &DataType, prefix: &str, paths: &mut Vec<String>) {
    match data_type {
        DataType::Struct(nested) => {
            collect_paths_recursive(nested, prefix, paths);
        }
        DataType::Array(arr) => {
            // Array elements use fixed name "element"
            let element_path = format!("{}.element", prefix);
            collect_paths_from_data_type(arr.element_type(), &element_path, paths);
        }
        DataType::Map(map) => {
            // Map keys use fixed name "key", values use "value"
            let key_path = format!("{}.key", prefix);
            let value_path = format!("{}.value", prefix);
            collect_paths_from_data_type(map.key_type(), &key_path, paths);
            collect_paths_from_data_type(map.value_type(), &value_path, paths);
        }
        _ => {}
    }
}

/// Checks for duplicate column names (case-insensitive).
///
/// # Arguments
/// * `paths` - All field paths in the schema
///
/// # Errors
/// Returns an error listing all duplicate columns found.
fn check_duplicate_columns(paths: &[String]) -> DeltaResult<()> {
    let mut seen = HashSet::new();
    let mut duplicates = Vec::new();

    for path in paths {
        let lower = path.to_lowercase();
        if !seen.insert(lower) {
            duplicates.push(path.clone());
        }
    }

    if !duplicates.is_empty() {
        duplicates.sort();
        return Err(Error::generic(format!(
            "Found duplicate column(s) (case-insensitive): [{}]",
            duplicates.join(", ")
        )));
    }

    Ok(())
}

/// Validates that column names contain only valid characters.
///
/// When column mapping is disabled, column names cannot contain Parquet special
/// characters: `[ ,;{}()\n\t=]`
///
/// When column mapping is enabled, only newline (`\n`) is forbidden.
///
/// # Arguments
/// * `schema` - The schema to validate
/// * `column_mapping_enabled` - Whether column mapping is enabled
///
/// # Errors
/// Returns an error if any column name contains invalid characters.
fn validate_column_names(schema: &StructType, column_mapping_enabled: bool) -> DeltaResult<()> {
    validate_names_recursive(schema, column_mapping_enabled)
}

/// Recursively validates column names in a struct type.
fn validate_names_recursive(
    struct_type: &StructType,
    column_mapping_enabled: bool,
) -> DeltaResult<()> {
    for field in struct_type.fields() {
        validate_single_column_name(field.name(), column_mapping_enabled)?;

        // Recurse into nested types.
        // Note: DataType variants like Struct(Box<StructType>) use Box for heap allocation.
        // When pattern matching, `nested` binds to &Box<StructType>, but Rust's deref coercion
        // automatically converts it to &StructType when passed to functions expecting that type.
        match field.data_type() {
            DataType::Struct(nested) => {
                validate_names_recursive(nested, column_mapping_enabled)?;
            }
            DataType::Array(arr) => {
                // Array<Struct<...>>: validate field names inside the element struct
                if let DataType::Struct(nested) = arr.element_type() {
                    validate_names_recursive(nested, column_mapping_enabled)?;
                }
            }
            DataType::Map(map) => {
                // Map<Struct<...>, V> or Map<K, Struct<...>>: validate field names in key/value structs
                if let DataType::Struct(nested) = map.key_type() {
                    validate_names_recursive(nested, column_mapping_enabled)?;
                }
                if let DataType::Struct(nested) = map.value_type() {
                    validate_names_recursive(nested, column_mapping_enabled)?;
                }
            }
            _ => {}
        }
    }

    Ok(())
}

/// Validates a single column name for invalid characters.
///
/// - With column mapping enabled: only newlines are forbidden
/// - Without column mapping: Parquet special characters are forbidden: `[ ,;{}()\n\t=]`
fn validate_single_column_name(name: &str, column_mapping_enabled: bool) -> DeltaResult<()> {
    if column_mapping_enabled {
        // With column mapping, only forbid newlines
        if name.contains(ALWAYS_INVALID_CHARS) {
            return Err(Error::generic(format!(
                "Column name '{}' contains invalid character: newline (\\n)",
                name
            )));
        }
    } else {
        // Without column mapping, forbid all Parquet special characters
        for &ch in INVALID_PARQUET_CHARS {
            if name.contains(ch) {
                let char_display = match ch {
                    '\n' => "\\n".to_string(),
                    '\t' => "\\t".to_string(),
                    ' ' => "space".to_string(),
                    c => c.to_string(),
                };
                return Err(Error::generic(format!(
                    "Column name '{}' contains invalid character for Parquet: {}. \
                     These characters are not allowed: [ ,;{{}}()\\n\\t=]",
                    name, char_display
                )));
            }
        }
    }
    Ok(())
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ArrayType, MapType, StructField};

    // -------------------------------------------------------------------------
    // Test fixtures
    // -------------------------------------------------------------------------

    fn simple_schema() -> StructType {
        StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
        ])
    }

    fn nested_schema() -> StructType {
        StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new(
                "address",
                DataType::Struct(Box::new(StructType::new_unchecked(vec![
                    StructField::new("street", DataType::STRING, true),
                    StructField::new("city", DataType::STRING, true),
                ]))),
                true,
            ),
        ])
    }

    // -------------------------------------------------------------------------
    // Tests for collect_all_field_paths
    // -------------------------------------------------------------------------

    #[test]
    fn test_collect_all_field_paths_simple() {
        let schema = simple_schema();
        let paths = collect_all_field_paths(&schema);
        assert_eq!(paths, vec!["id", "name"]);
    }

    #[test]
    fn test_collect_all_field_paths_nested() {
        let schema = nested_schema();
        let paths = collect_all_field_paths(&schema);
        assert_eq!(
            paths,
            vec!["id", "address", "address.street", "address.city"]
        );
    }

    #[test]
    fn test_collect_all_field_paths_array_of_structs() {
        let schema = StructType::new_unchecked(vec![StructField::new(
            "items",
            DataType::Array(Box::new(ArrayType::new(
                DataType::Struct(Box::new(StructType::new_unchecked(vec![
                    StructField::new("x", DataType::INTEGER, false),
                    StructField::new("y", DataType::STRING, true),
                ]))),
                true,
            ))),
            true,
        )]);
        let paths = collect_all_field_paths(&schema);
        assert_eq!(paths, vec!["items", "items.element.x", "items.element.y"]);
    }

    #[test]
    fn test_collect_all_field_paths_map_with_struct_value() {
        let schema = StructType::new_unchecked(vec![StructField::new(
            "data",
            DataType::Map(Box::new(MapType::new(
                DataType::STRING,
                DataType::Struct(Box::new(StructType::new_unchecked(vec![StructField::new(
                    "value_field",
                    DataType::INTEGER,
                    false,
                )]))),
                true,
            ))),
            true,
        )]);
        let paths = collect_all_field_paths(&schema);
        assert_eq!(paths, vec!["data", "data.value.value_field"]);
    }

    #[test]
    fn test_check_duplicate_columns_inside_array_struct() {
        // Duplicate field names inside a struct that's an array element
        let schema = StructType::new_unchecked(vec![StructField::new(
            "items",
            DataType::Array(Box::new(ArrayType::new(
                DataType::Struct(Box::new(StructType::new_unchecked(vec![
                    StructField::new("ID", DataType::INTEGER, false),
                    StructField::new("id", DataType::STRING, true), // duplicate!
                ]))),
                true,
            ))),
            true,
        )]);
        let result = validate_schema_for_create(&schema, ColumnMappingMode::None);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("duplicate"));
    }

    // -------------------------------------------------------------------------
    // Tests for check_duplicate_columns
    // -------------------------------------------------------------------------

    #[test]
    fn test_check_duplicate_columns_no_duplicates() {
        let paths = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        assert!(check_duplicate_columns(&paths).is_ok());
    }

    #[test]
    fn test_check_duplicate_columns_case_insensitive() {
        let paths = vec!["Name".to_string(), "name".to_string()];
        let result = check_duplicate_columns(&paths);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("duplicate"));
        assert!(err.contains("name"));
    }

    #[test]
    fn test_check_duplicate_columns_nested() {
        let paths = vec![
            "a".to_string(),
            "b".to_string(),
            "a.c".to_string(),
            "A.C".to_string(), // duplicate of a.c
        ];
        let result = check_duplicate_columns(&paths);
        assert!(result.is_err());
    }

    // -------------------------------------------------------------------------
    // Tests for validate_column_names
    // -------------------------------------------------------------------------

    #[test]
    fn test_validate_column_names_valid() {
        let schema = simple_schema();
        assert!(validate_column_names(&schema, false).is_ok());
        assert!(validate_column_names(&schema, true).is_ok());
    }

    #[test]
    fn test_validate_column_names_space_no_cm() {
        let schema =
            StructType::new_unchecked(vec![StructField::new("my column", DataType::STRING, true)]);
        let result = validate_column_names(&schema, false);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("space"));
    }

    #[test]
    fn test_validate_column_names_space_with_cm() {
        // With column mapping, spaces are allowed
        let schema =
            StructType::new_unchecked(vec![StructField::new("my column", DataType::STRING, true)]);
        assert!(validate_column_names(&schema, true).is_ok());
    }

    #[test]
    fn test_validate_column_names_newline_always_invalid() {
        let schema =
            StructType::new_unchecked(vec![StructField::new("my\ncolumn", DataType::STRING, true)]);
        // Invalid without column mapping
        assert!(validate_column_names(&schema, false).is_err());
        // Also invalid with column mapping
        assert!(validate_column_names(&schema, true).is_err());
    }

    #[test]
    fn test_validate_column_names_special_chars_no_cm() {
        for ch in [',', ';', '{', '}', '(', ')', '\t', '='] {
            let name = format!("col{}name", ch);
            let schema =
                StructType::new_unchecked(vec![StructField::new(&name, DataType::STRING, true)]);
            let result = validate_column_names(&schema, false);
            assert!(
                result.is_err(),
                "Character '{}' should be invalid without CM",
                ch
            );
        }
    }

    #[test]
    fn test_validate_column_names_nested() {
        let schema = StructType::new_unchecked(vec![StructField::new(
            "outer",
            DataType::Struct(Box::new(StructType::new_unchecked(vec![StructField::new(
                "inner column", // has space
                DataType::STRING,
                true,
            )]))),
            true,
        )]);
        let result = validate_column_names(&schema, false);
        assert!(result.is_err());
    }

    // -------------------------------------------------------------------------
    // Tests for validate_schema_for_create
    // -------------------------------------------------------------------------

    #[test]
    fn test_validate_schema_for_create_valid() {
        let schema = simple_schema();
        assert!(validate_schema_for_create(&schema, ColumnMappingMode::None).is_ok());
    }

    #[test]
    fn test_validate_schema_for_create_duplicate() {
        // StructType::new_unchecked allows duplicates for testing
        let schema = StructType::new_unchecked(vec![
            StructField::new("Name", DataType::STRING, true),
            StructField::new("name", DataType::STRING, true),
        ]);
        let result = validate_schema_for_create(&schema, ColumnMappingMode::None);
        assert!(result.is_err());
    }

    // -------------------------------------------------------------------------
    // Tests for is_valid_partition_column_type
    // -------------------------------------------------------------------------

    #[test]
    fn test_is_valid_partition_column_type() {
        // Primitive types are valid
        assert!(is_valid_partition_column_type(&DataType::INTEGER));
        assert!(is_valid_partition_column_type(&DataType::STRING));
        assert!(is_valid_partition_column_type(&DataType::BOOLEAN));
        assert!(is_valid_partition_column_type(&DataType::DATE));
        assert!(is_valid_partition_column_type(&DataType::TIMESTAMP));

        // Complex types are invalid
        assert!(!is_valid_partition_column_type(&DataType::Struct(
            Box::new(simple_schema())
        )));
        assert!(!is_valid_partition_column_type(&DataType::Array(Box::new(
            ArrayType::new(DataType::STRING, true)
        ))));
        assert!(!is_valid_partition_column_type(&DataType::Map(Box::new(
            MapType::new(DataType::STRING, DataType::STRING, true)
        ))));
    }

    // -------------------------------------------------------------------------
    // Tests for validate_partition_columns
    // -------------------------------------------------------------------------

    #[test]
    fn test_validate_partition_columns_valid() {
        let schema = simple_schema();
        assert!(validate_partition_columns(&schema, &["id".to_string()]).is_ok());
        assert!(validate_partition_columns(&schema, &["name".to_string()]).is_ok());
        assert!(
            validate_partition_columns(&schema, &["id".to_string(), "name".to_string()]).is_ok()
        );
    }

    #[test]
    fn test_validate_partition_columns_not_found() {
        let schema = simple_schema();
        let result = validate_partition_columns(&schema, &["nonexistent".to_string()]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("not found"));
    }

    #[test]
    fn test_validate_partition_columns_case_insensitive() {
        let schema = simple_schema();
        // Should find "id" even when specified as "ID"
        assert!(validate_partition_columns(&schema, &["ID".to_string()]).is_ok());
        assert!(validate_partition_columns(&schema, &["Name".to_string()]).is_ok());
    }

    #[test]
    fn test_validate_partition_columns_complex_type() {
        let schema = StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new(
                "tags",
                DataType::Array(Box::new(ArrayType::new(DataType::STRING, true))),
                true,
            ),
        ]);
        let result = validate_partition_columns(&schema, &["tags".to_string()]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unsupported type"));
    }

    #[test]
    fn test_validate_partition_columns_struct_type() {
        let schema = nested_schema();
        let result = validate_partition_columns(&schema, &["address".to_string()]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unsupported type"));
    }

    #[test]
    fn test_validate_partition_columns_map_type() {
        let schema = StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new(
                "metadata",
                DataType::Map(Box::new(MapType::new(
                    DataType::STRING,
                    DataType::STRING,
                    true,
                ))),
                true,
            ),
        ]);
        let result = validate_partition_columns(&schema, &["metadata".to_string()]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unsupported type"));
    }
}
