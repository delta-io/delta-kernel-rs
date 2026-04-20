//! Schema validation utilities for Delta table creation.
//!
//! Validates schemas per the Delta protocol specification.

use std::collections::HashSet;

use crate::schema::{StructField, StructType};
use crate::table_features::ColumnMappingMode;
use crate::transforms::SchemaTransform;
use crate::transform_output_type;
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
/// 4. Rejects non-null columns when the `invariants` writer feature is not enabled
pub(crate) fn validate_schema_for_create(
    schema: &StructType,
    column_mapping_mode: ColumnMappingMode,
    invariants_enabled: bool,
) -> DeltaResult<()> {
    if schema.num_fields() == 0 {
        return Err(Error::generic("Schema cannot be empty"));
    }
    let mut validator = SchemaValidator::new(column_mapping_mode, invariants_enabled);
    // We reuse the SchemaTransform trait for its recursive traversal machinery.
    // The validator never transforms the schema -- it only inspects fields and
    // collects errors. The return value is intentionally discarded.
    validator.transform_struct(schema);
    validator.into_result()
}

/// Schema visitor that validates field names and detects duplicates.
///
/// Implements `SchemaTransform` to reuse the existing recursive struct/array/map traversal.
/// Collects all validation errors so the caller gets a complete list of violations in a
/// single error message.
///
/// Note: `StructType::try_new` already catches same-level case-insensitive duplicates.
/// This validator additionally detects cross-level path duplicates and catches schemas
/// built with `new_unchecked`.
struct SchemaValidator {
    cm_enabled: bool,
    invariants_enabled: bool,
    seen_paths: HashSet<String>,
    current_path: Vec<String>,
    errors: Vec<String>,
}

impl SchemaValidator {
    fn new(column_mapping_mode: ColumnMappingMode, invariants_enabled: bool) -> Self {
        Self {
            cm_enabled: !matches!(column_mapping_mode, ColumnMappingMode::None),
            invariants_enabled,
            seen_paths: HashSet::new(),
            current_path: Vec::new(),
            errors: Vec::new(),
        }
    }

    fn into_result(self) -> DeltaResult<()> {
        if self.errors.is_empty() {
            Ok(())
        } else {
            Err(Error::generic(format!(
                "Schema validation failed:\n- {}",
                self.errors.join("\n- ")
            )))
        }
    }
}

impl<'a> SchemaTransform<'a> for SchemaValidator {
    transform_output_type!(|'a, T| ());

    /// The default `transform_variant` recurses into the variant's struct fields
    /// (metadata, value) via `recurse_into_struct`. Those fields are protocol-defined
    /// and must be non-null -- they are not user-controlled schema columns. We override
    /// to return the variant struct unchanged, skipping recursion so the non-null check
    /// in `transform_struct_field` does not reject these fixed internal fields.
    fn transform_variant(&mut self, _stype: &'a StructType) {}

    fn transform_struct_field(&mut self, field: &'a StructField) {
        if let Err(e) = validate_field_name(field.name(), self.cm_enabled) {
            self.errors.push(e.to_string());
        }

        // Check duplicate paths. We use a null-byte separator instead of dots because
        // column names can contain literal dots when column mapping is enabled. A dot
        // separator would make column "a.b" indistinguishable from nested field b in
        // struct a. Null bytes cannot appear in column names, so they are safe to use.
        self.current_path.push(field.name().to_ascii_lowercase());
        if !self.invariants_enabled && !field.is_nullable() {
            self.errors.push(format!(
                "Non-null column '{}' is not supported during CREATE TABLE unless \
                 writer feature 'invariants' is enabled",
                self.current_path.join(".")
            ));
        }
        let key = self.current_path.join("\0");
        if !self.seen_paths.insert(key) {
            self.errors.push(format!(
                "Schema contains duplicate column (case-insensitive): '{}'",
                field.name()
            ));
        }

        self.recurse_into_struct_field(field);
        self.current_path.pop();
    }
}

/// Validates an individual field name.
///
/// When column mapping is disabled, rejects names containing Parquet special characters.
/// When column mapping is enabled, only rejects newlines since physical names are
/// auto-generated but newlines in column names break metadata serialization regardless
/// of column mapping mode.
fn validate_field_name(name: &str, cm_enabled: bool) -> DeltaResult<()> {
    if name.is_empty() {
        return Err(Error::generic("Column name cannot be empty"));
    }
    if cm_enabled {
        // Newlines break metadata serialization regardless of column mapping mode.
        if name.contains('\n') {
            return Err(Error::generic(format!(
                "Column name '{name}' contains a newline character, which is not allowed"
            )));
        }
    } else if name.contains(INVALID_PARQUET_CHARS) {
        let invalid: Vec<char> = name
            .chars()
            .filter(|c| INVALID_PARQUET_CHARS.contains(c))
            .collect();
        return Err(Error::generic(format!(
            "Column name '{name}' contains invalid character(s) {invalid:?} that are not \
             allowed in Parquet column names. \
             Enable column mapping to use special characters in column names."
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::schema::{ArrayType, DataType, MapType, StructField, StructType};

    // === Schema builders for test cases ===

    fn simple_schema() -> StructType {
        StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
        ])
    }

    fn schema_with_underscores() -> StructType {
        StructType::new_unchecked(vec![
            StructField::new("col_1", DataType::INTEGER, false),
            StructField::new("_private", DataType::STRING, true),
            StructField::new("CamelCase123", DataType::LONG, false),
        ])
    }

    fn schema_with_special_chars() -> StructType {
        StructType::new_unchecked(vec![
            StructField::new("my column", DataType::INTEGER, false),
            StructField::new("col;name", DataType::STRING, true),
        ])
    }

    fn schema_with_dot() -> StructType {
        StructType::new_unchecked(vec![
            StructField::new("a.b", DataType::INTEGER, false),
            StructField::new("c", DataType::STRING, true),
        ])
    }

    fn schema_different_struct_children() -> StructType {
        let inner_a =
            StructType::new_unchecked(vec![StructField::new("child", DataType::INTEGER, false)]);
        let inner_b =
            StructType::new_unchecked(vec![StructField::new("CHILD", DataType::STRING, true)]);
        StructType::new_unchecked(vec![
            StructField::new("a", DataType::Struct(Box::new(inner_a)), false),
            StructField::new("b", DataType::Struct(Box::new(inner_b)), false),
        ])
    }

    fn schema_with_space() -> StructType {
        StructType::new_unchecked(vec![StructField::new(
            "my column",
            DataType::INTEGER,
            false,
        )])
    }

    fn schema_with_semicolon() -> StructType {
        StructType::new_unchecked(vec![StructField::new("col;name", DataType::INTEGER, false)])
    }

    fn schema_with_newline() -> StructType {
        StructType::new_unchecked(vec![StructField::new(
            "col\nname",
            DataType::INTEGER,
            false,
        )])
    }

    fn schema_with_empty_name() -> StructType {
        StructType::new_unchecked(vec![StructField::new("", DataType::INTEGER, false)])
    }

    fn schema_nested_bad_char() -> StructType {
        let inner = StructType::new_unchecked(vec![StructField::new(
            "bad column",
            DataType::INTEGER,
            false,
        )]);
        StructType::new_unchecked(vec![StructField::new(
            "parent",
            DataType::Struct(Box::new(inner)),
            false,
        )])
    }

    fn schema_array_bad_char() -> StructType {
        let inner =
            StructType::new_unchecked(vec![StructField::new("bad col", DataType::INTEGER, false)]);
        StructType::new_unchecked(vec![StructField::new(
            "arr",
            DataType::Array(Box::new(ArrayType::new(
                DataType::Struct(Box::new(inner)),
                true,
            ))),
            false,
        )])
    }

    fn schema_map_bad_char() -> StructType {
        let inner =
            StructType::new_unchecked(vec![StructField::new("bad;val", DataType::INTEGER, false)]);
        StructType::new_unchecked(vec![StructField::new(
            "m",
            DataType::Map(Box::new(MapType::new(
                DataType::STRING,
                DataType::Struct(Box::new(inner)),
                true,
            ))),
            false,
        )])
    }

    fn schema_top_level_dup() -> StructType {
        let inner =
            StructType::new_unchecked(vec![StructField::new("x", DataType::INTEGER, false)]);
        StructType::new_unchecked(vec![
            StructField::new("a", DataType::Struct(Box::new(inner)), false),
            StructField::new("A", DataType::STRING, true),
        ])
    }

    fn schema_array_dup() -> StructType {
        let inner = StructType::new_unchecked(vec![
            StructField::new("x", DataType::INTEGER, false),
            StructField::new("X", DataType::STRING, true),
        ]);
        StructType::new_unchecked(vec![StructField::new(
            "arr",
            DataType::Array(Box::new(ArrayType::new(
                DataType::Struct(Box::new(inner)),
                true,
            ))),
            false,
        )])
    }

    fn schema_multi_bad() -> StructType {
        StructType::new_unchecked(vec![
            StructField::new("good", DataType::INTEGER, false),
            StructField::new("bad column", DataType::STRING, true),
            StructField::new("col;name", DataType::LONG, false),
        ])
    }

    fn schema_all_nullable() -> StructType {
        StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, true),
            StructField::new("name", DataType::STRING, true),
        ])
    }

    fn schema_top_level_non_null() -> StructType {
        StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
        ])
    }

    fn schema_nested_non_null() -> StructType {
        let nested =
            StructType::new_unchecked(vec![StructField::new("child", DataType::INTEGER, false)]);
        StructType::new_unchecked(vec![StructField::new(
            "parent",
            DataType::Struct(Box::new(nested)),
            true,
        )])
    }

    fn schema_array_nested_non_null() -> StructType {
        let nested =
            StructType::new_unchecked(vec![StructField::new("child", DataType::INTEGER, false)]);
        StructType::new_unchecked(vec![StructField::new(
            "arr",
            DataType::Array(Box::new(ArrayType::new(
                DataType::Struct(Box::new(nested)),
                true,
            ))),
            true,
        )])
    }

    fn schema_map_nested_non_null() -> StructType {
        let nested =
            StructType::new_unchecked(vec![StructField::new("child", DataType::INTEGER, false)]);
        StructType::new_unchecked(vec![StructField::new(
            "map",
            DataType::Map(Box::new(MapType::new(
                DataType::STRING,
                DataType::Struct(Box::new(nested)),
                true,
            ))),
            true,
        )])
    }

    // === Valid schemas ===

    #[rstest]
    #[case::simple(simple_schema(), ColumnMappingMode::None)]
    #[case::underscores_digits(schema_with_underscores(), ColumnMappingMode::None)]
    #[case::special_chars_with_cm(schema_with_special_chars(), ColumnMappingMode::Name)]
    #[case::dot_in_name_with_cm(schema_with_dot(), ColumnMappingMode::Name)]
    #[case::different_struct_children(schema_different_struct_children(), ColumnMappingMode::None)]
    fn valid_schema_accepted(#[case] schema: StructType, #[case] cm: ColumnMappingMode) {
        assert!(validate_schema_for_create(&schema, cm, true).is_ok());
    }

    // === Invalid schemas ===

    #[rstest]
    #[case::empty_schema(StructType::new_unchecked(vec![]), ColumnMappingMode::None, &["cannot be empty"])]
    #[case::space_without_cm(schema_with_space(), ColumnMappingMode::None, &["invalid character"])]
    #[case::semicolon_without_cm(schema_with_semicolon(), ColumnMappingMode::None, &["invalid character"])]
    #[case::newline_with_cm(schema_with_newline(), ColumnMappingMode::Name, &["newline"])]
    #[case::empty_name(schema_with_empty_name(), ColumnMappingMode::None, &["cannot be empty"])]
    #[case::nested_struct_bad_char(schema_nested_bad_char(), ColumnMappingMode::None, &["invalid character"])]
    #[case::array_nested_bad_char(schema_array_bad_char(), ColumnMappingMode::None, &["invalid character"])]
    #[case::map_nested_bad_char(schema_map_bad_char(), ColumnMappingMode::None, &["invalid character"])]
    #[case::top_level_dup(schema_top_level_dup(), ColumnMappingMode::None, &["duplicate"])]
    #[case::array_element_dup(schema_array_dup(), ColumnMappingMode::None, &["duplicate"])]
    #[case::multi_error(schema_multi_bad(), ColumnMappingMode::None, &["bad column", "col;name"])]
    fn invalid_schema_rejected(
        #[case] schema: StructType,
        #[case] cm: ColumnMappingMode,
        #[case] expected_errs: &[&str],
    ) {
        let result = validate_schema_for_create(&schema, cm, true);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        for expected in expected_errs {
            assert!(
                err.contains(expected),
                "Expected '{expected}' in error, got: {err}"
            );
        }
    }

    #[rstest]
    #[case::all_nullable_invariants_disabled(schema_all_nullable(), false, true, None)]
    #[case::top_level_non_null_invariants_disabled(
        schema_top_level_non_null(),
        false,
        false,
        Some("id")
    )]
    #[case::nested_non_null_invariants_disabled(
        schema_nested_non_null(),
        false,
        false,
        Some("parent.child")
    )]
    #[case::array_nested_non_null_invariants_disabled(
        schema_array_nested_non_null(),
        false,
        false,
        Some("arr.child")
    )]
    #[case::map_nested_non_null_invariants_disabled(
        schema_map_nested_non_null(),
        false,
        false,
        Some("map.child")
    )]
    #[case::top_level_non_null_invariants_enabled(schema_top_level_non_null(), true, true, None)]
    #[case::nested_non_null_invariants_enabled(schema_nested_non_null(), true, true, None)]
    fn non_null_columns_require_invariants_feature(
        #[case] schema: StructType,
        #[case] invariants_enabled: bool,
        #[case] expect_ok: bool,
        #[case] expected_path: Option<&str>,
    ) {
        let result =
            validate_schema_for_create(&schema, ColumnMappingMode::None, invariants_enabled);

        if expect_ok {
            assert!(result.is_ok(), "expected success, got {result:?}");
            return;
        }

        let err = result.expect_err("expected non-null validation error");
        let msg = err.to_string();
        assert!(
            msg.contains("Non-null column"),
            "Expected non-null validation error, got: {msg}"
        );
        if let Some(path) = expected_path {
            assert!(
                msg.contains(path),
                "Expected path '{path}' in error message, got: {msg}"
            );
        }
        assert!(
            msg.contains("writer feature 'invariants'"),
            "Expected invariants guidance in error message, got: {msg}"
        );
    }
}
