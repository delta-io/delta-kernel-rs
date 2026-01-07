//! Schema validation utilities for Delta tables.
//!
//! This module provides validation functions to ensure schemas conform to Delta Lake
//! protocol requirements before creating or modifying tables.

use std::collections::HashSet;

use crate::error::CreateTableError;
use crate::schema::{ColumnName, DataType, PrimitiveType, StructType};
use crate::table_features::ColumnMappingMode;
use crate::{DeltaResult, Error};

/// Maximum number of clustering columns allowed per Delta specification.
pub(crate) const MAX_CLUSTERING_COLUMNS: usize = 4;

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
                CreateTableError::invalid_partition_column(col_name, "not found in schema")
            })?;

        // Check that the data type is valid for partitioning
        if !is_valid_partition_column_type(field.data_type()) {
            return Err(CreateTableError::invalid_partition_column(
                col_name,
                format!(
                    "unsupported type '{}'. Only primitive types \
                     (string, integer, long, boolean, date, timestamp, etc.) are allowed",
                    field.data_type()
                ),
            )
            .into());
        }
    }

    Ok(())
}

/// Validates clustering columns against the schema and constraints.
///
/// This performs the following validations:
/// 1. No more than [`MAX_CLUSTERING_COLUMNS`] (4) clustering columns
/// 2. Each clustering column exists in the schema (supports nested paths)
/// 3. Nested paths traverse through struct fields only
/// 4. Each clustering column has a data-skipping-eligible type
///
/// # Arguments
/// * `columns` - The clustering column names (may include nested paths like `a.b.c`)
/// * `schema` - The table schema
///
/// # Errors
/// Returns an error if:
/// - More than 4 clustering columns are specified
/// - A clustering column doesn't exist in the schema
/// - A clustering column path tries to traverse a non-struct field
/// - A clustering column has a non-skipping-eligible type (boolean, binary, complex types)
pub(crate) fn validate_clustering_columns(
    columns: &[ColumnName],
    schema: &StructType,
) -> DeltaResult<()> {
    // Check maximum clustering columns
    if columns.len() > MAX_CLUSTERING_COLUMNS {
        return Err(CreateTableError::InvalidLayoutColumn {
            layout_type: "clustering",
            column: format!("{} columns", columns.len()),
            reason: format!(
                "cannot specify more than {} clustering columns",
                MAX_CLUSTERING_COLUMNS
            ),
        }
        .into());
    }

    // Validate each column exists in the schema by traversing the path
    for col in columns {
        let path = col.path();
        if path.is_empty() {
            return Err(CreateTableError::invalid_clustering_column(
                col,
                "column path cannot be empty",
            )
            .into());
        }

        // Traverse the schema tree to validate the path and get the final field type
        let mut current_schema = schema;
        let mut final_data_type: Option<&DataType> = None;

        for (i, field_name) in path.iter().enumerate() {
            match current_schema.field(field_name) {
                Some(field) => {
                    if i < path.len() - 1 {
                        // Not the last element - must be a struct to traverse into
                        match field.data_type() {
                            DataType::Struct(inner) => {
                                current_schema = inner;
                            }
                            _ => {
                                return Err(CreateTableError::invalid_clustering_column(
                                    col,
                                    format!(
                                        "field '{}' is not a struct and cannot contain nested fields",
                                        field_name
                                    ),
                                )
                                .into());
                            }
                        }
                    } else {
                        // Last element - capture the data type
                        final_data_type = Some(field.data_type());
                    }
                }
                None => {
                    return Err(CreateTableError::invalid_clustering_column(
                        col,
                        format!("field '{}' does not exist in schema", field_name),
                    )
                    .into());
                }
            }
        }

        // Validate the final column type is skipping-eligible
        if let Some(data_type) = final_data_type {
            if !is_skipping_eligible_type(data_type) {
                return Err(CreateTableError::invalid_clustering_column(
                    col,
                    format!(
                        "unsupported type '{}'. Clustering columns must have data-skipping-eligible types \
                         (numeric, string, date, timestamp, or decimal). \
                         Boolean, binary, and complex types are not supported",
                        data_type
                    ),
                )
                .into());
            }
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

/// Checks if a data type is eligible for data skipping (min/max statistics).
///
/// Clustering relies on min/max statistics to skip files during queries. For example,
/// if a file has `min=100, max=199` for an integer column, a query `WHERE col = 250`
/// can skip the entire file without reading it. This only works for types where
/// min/max comparisons are meaningful.
///
/// # Eligible types
/// - Numeric: byte, short, integer, long, float, double, decimal
/// - String: string (lexicographic ordering)
/// - Temporal: date, timestamp, timestamp_ntz
///
/// # NOT eligible
/// - boolean: only 2 values, so min=false/max=true in most files → can't skip anything
/// - binary: arbitrary bytes with no meaningful ordering for range comparisons
/// - Complex types (struct, array, map, variant): no single min/max value for compounds
fn is_skipping_eligible_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::Primitive(p) => !matches!(p, PrimitiveType::Boolean | PrimitiveType::Binary),
        _ => false, // Struct, Array, Map, Variant are not skipping-eligible
    }
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

    /// Tests all primitive data types that SHOULD be accepted for partitioning.
    /// Unlike clustering, partition columns accept ALL primitive types including boolean/binary.
    #[test]
    fn test_validate_partition_columns_supported_types() {
        let schema = StructType::new_unchecked(vec![
            // Numeric types
            StructField::new("col_byte", DataType::BYTE, true),
            StructField::new("col_short", DataType::SHORT, true),
            StructField::new("col_integer", DataType::INTEGER, true),
            StructField::new("col_long", DataType::LONG, true),
            StructField::new("col_float", DataType::FLOAT, true),
            StructField::new("col_double", DataType::DOUBLE, true),
            StructField::new("col_decimal", DataType::decimal(10, 2).unwrap(), true),
            // String and binary types
            StructField::new("col_string", DataType::STRING, true),
            StructField::new("col_binary", DataType::BINARY, true),
            StructField::new("col_boolean", DataType::BOOLEAN, true),
            // Temporal types
            StructField::new("col_date", DataType::DATE, true),
            StructField::new("col_timestamp", DataType::TIMESTAMP, true),
            StructField::new("col_timestamp_ntz", DataType::TIMESTAMP_NTZ, true),
        ]);

        // All primitive types should succeed
        let supported_columns = [
            "col_byte",
            "col_short",
            "col_integer",
            "col_long",
            "col_float",
            "col_double",
            "col_decimal",
            "col_string",
            "col_binary",
            "col_boolean",
            "col_date",
            "col_timestamp",
            "col_timestamp_ntz",
        ];
        for col in supported_columns {
            assert!(
                validate_partition_columns(&schema, &[col.to_string()]).is_ok(),
                "Expected {} to be a valid partition column",
                col
            );
        }

        // Multiple columns should work
        assert!(validate_partition_columns(
            &schema,
            &[
                "col_integer".to_string(),
                "col_string".to_string(),
                "col_date".to_string()
            ]
        )
        .is_ok());
    }

    /// Tests all complex data types that should be REJECTED for partitioning.
    /// Partition columns must be primitive types only.
    #[test]
    fn test_validate_partition_columns_unsupported_types() {
        let schema = StructType::new_unchecked(vec![
            StructField::new(
                "col_struct",
                DataType::Struct(Box::new(StructType::new_unchecked(vec![StructField::new(
                    "field",
                    DataType::STRING,
                    true,
                )]))),
                true,
            ),
            StructField::new(
                "col_array",
                DataType::Array(Box::new(ArrayType::new(DataType::STRING, true))),
                true,
            ),
            StructField::new(
                "col_map",
                DataType::Map(Box::new(MapType::new(
                    DataType::STRING,
                    DataType::STRING,
                    true,
                ))),
                true,
            ),
        ]);

        let unsupported_cases = [
            ("col_struct", "struct"),
            ("col_array", "array"),
            ("col_map", "map"),
        ];

        for (col, type_name) in unsupported_cases {
            let result = validate_partition_columns(&schema, &[col.to_string()]);
            assert!(
                result.is_err(),
                "Expected {} ({}) to be rejected as partition column",
                col,
                type_name
            );
            let err = result.unwrap_err().to_string();
            assert!(
                err.contains("unsupported type"),
                "Expected 'unsupported type' in error for {}: {}",
                col,
                err
            );
        }
    }

    /// Tests structural validations: column existence, case-insensitive lookup.
    #[test]
    fn test_validate_partition_columns_structural() {
        let schema = simple_schema(); // has "id" and "name"

        // Column must exist
        let result = validate_partition_columns(&schema, &["nonexistent".to_string()]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));

        // Case-insensitive lookup should work
        assert!(validate_partition_columns(&schema, &["ID".to_string()]).is_ok());
        assert!(validate_partition_columns(&schema, &["Name".to_string()]).is_ok());
        assert!(validate_partition_columns(&schema, &["NAME".to_string()]).is_ok());
    }

    // -------------------------------------------------------------------------
    // Tests for validate_clustering_columns
    // -------------------------------------------------------------------------

    /// Tests all skipping-eligible data types that SHOULD be accepted for clustering.
    /// These types support min/max statistics for efficient data skipping.
    #[test]
    fn test_validate_clustering_columns_supported_types() {
        let schema = StructType::new_unchecked(vec![
            // Numeric types
            StructField::new("col_byte", DataType::BYTE, true),
            StructField::new("col_short", DataType::SHORT, true),
            StructField::new("col_integer", DataType::INTEGER, true),
            StructField::new("col_long", DataType::LONG, true),
            StructField::new("col_float", DataType::FLOAT, true),
            StructField::new("col_double", DataType::DOUBLE, true),
            StructField::new("col_decimal", DataType::decimal(10, 2).unwrap(), true),
            // String type
            StructField::new("col_string", DataType::STRING, true),
            // Temporal types
            StructField::new("col_date", DataType::DATE, true),
            StructField::new("col_timestamp", DataType::TIMESTAMP, true),
            StructField::new("col_timestamp_ntz", DataType::TIMESTAMP_NTZ, true),
            // Nested struct with eligible leaf types
            StructField::new(
                "nested",
                DataType::Struct(Box::new(StructType::new_unchecked(vec![
                    StructField::new("inner_string", DataType::STRING, true),
                    StructField::new("inner_long", DataType::LONG, true),
                ]))),
                true,
            ),
        ]);

        // All these should succeed
        let supported_columns = [
            "col_byte",
            "col_short",
            "col_integer",
            "col_long",
            "col_float",
            "col_double",
            "col_decimal",
            "col_string",
            "col_date",
            "col_timestamp",
            "col_timestamp_ntz",
        ];
        for col in supported_columns {
            assert!(
                validate_clustering_columns(&[ColumnName::new([col])], &schema).is_ok(),
                "Expected {} to be a valid clustering column",
                col
            );
        }

        // Nested columns with eligible types should also work
        assert!(validate_clustering_columns(
            &[ColumnName::new(["nested", "inner_string"])],
            &schema
        )
        .is_ok());
        assert!(
            validate_clustering_columns(&[ColumnName::new(["nested", "inner_long"])], &schema)
                .is_ok()
        );

        // Multiple columns should work
        assert!(validate_clustering_columns(
            &[
                ColumnName::new(["col_integer"]),
                ColumnName::new(["col_string"]),
                ColumnName::new(["col_date"]),
            ],
            &schema
        )
        .is_ok());
    }

    /// Tests all data types that should be REJECTED for clustering.
    /// These types don't support meaningful min/max statistics for data skipping.
    #[test]
    fn test_validate_clustering_columns_unsupported_types() {
        let schema = StructType::new_unchecked(vec![
            // Primitive types without meaningful min/max
            StructField::new("col_boolean", DataType::BOOLEAN, true),
            StructField::new("col_binary", DataType::BINARY, true),
            // Complex types
            StructField::new(
                "col_struct",
                DataType::Struct(Box::new(StructType::new_unchecked(vec![StructField::new(
                    "field",
                    DataType::STRING,
                    true,
                )]))),
                true,
            ),
            StructField::new(
                "col_array",
                DataType::Array(Box::new(ArrayType::new(DataType::STRING, true))),
                true,
            ),
            StructField::new(
                "col_map",
                DataType::Map(Box::new(MapType::new(
                    DataType::STRING,
                    DataType::STRING,
                    true,
                ))),
                true,
            ),
        ]);

        let unsupported_cases = [
            ("col_boolean", "boolean"),
            ("col_binary", "binary"),
            ("col_struct", "struct"),
            ("col_array", "array"),
            ("col_map", "map"),
        ];

        for (col, type_name) in unsupported_cases {
            let result = validate_clustering_columns(&[ColumnName::new([col])], &schema);
            assert!(
                result.is_err(),
                "Expected {} ({}) to be rejected as clustering column",
                col,
                type_name
            );
            let err = result.unwrap_err().to_string();
            assert!(
                err.contains("unsupported type"),
                "Expected 'unsupported type' in error for {}: {}",
                col,
                err
            );
        }
    }

    /// Tests structural validations: max columns, column existence, nested path traversal.
    #[test]
    fn test_validate_clustering_columns_structural() {
        let schema = StructType::new_unchecked(vec![
            StructField::new("a", DataType::INTEGER, false),
            StructField::new("b", DataType::INTEGER, false),
            StructField::new("c", DataType::INTEGER, false),
            StructField::new("d", DataType::INTEGER, false),
            StructField::new("e", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
        ]);

        // Max 4 columns allowed
        let result = validate_clustering_columns(
            &[
                ColumnName::new(["a"]),
                ColumnName::new(["b"]),
                ColumnName::new(["c"]),
                ColumnName::new(["d"]),
                ColumnName::new(["e"]),
            ],
            &schema,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("more than 4"));

        // 4 columns is allowed
        assert!(validate_clustering_columns(
            &[
                ColumnName::new(["a"]),
                ColumnName::new(["b"]),
                ColumnName::new(["c"]),
                ColumnName::new(["d"]),
            ],
            &schema
        )
        .is_ok());

        // Column must exist
        let result = validate_clustering_columns(&[ColumnName::new(["nonexistent"])], &schema);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not exist"));

        // Cannot traverse into non-struct field
        let result = validate_clustering_columns(&[ColumnName::new(["name", "invalid"])], &schema);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("is not a struct"));
    }
}
