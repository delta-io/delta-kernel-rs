//! Schema validation utilities shared by table creation and schema evolution.
//!
//! Validates schemas per the Delta protocol specification.

use std::collections::HashSet;

use crate::schema::{StructField, StructType};
use crate::table_features::ColumnMappingMode;
use crate::transforms::SchemaTransform;
use crate::{transform_output_type, DeltaResult, KernelError};

/// Characters that are invalid in Parquet column names when column mapping is disabled.
/// These characters have special meaning in Parquet schema syntax.
const INVALID_PARQUET_CHARS: &[char] = &[' ', ',', ';', '{', '}', '(', ')', '\n', '\t', '='];

/// Validates a schema for CREATE TABLE or ALTER TABLE.
///
/// Performs the following checks:
/// 1. No duplicate column names (case-insensitive, including nested fields)
/// 2. Column names contain only valid characters
/// 3. Rejects fields with `delta.invariants` metadata (SQL expression invariants are not supported
///    by kernel)
pub(crate) fn validate_schema(
    schema: &StructType,
    column_mapping_mode: ColumnMappingMode,
) -> DeltaResult<()> {
    let mut validator = SchemaValidator::new(column_mapping_mode);
    // We reuse the SchemaTransform trait for its recursive traversal machinery.
    // The validator never transforms the schema -- it only inspects fields and
    // collects errors. The return value is intentionally discarded.
    validator.transform_struct(schema);
    validator.into_result()
}

/// Schema visitor that validates field names, detects duplicates, and rejects
/// unsupported column metadata.
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
    seen_paths: HashSet<String>,
    current_path: Vec<String>,
    errors: Vec<String>,
}

impl SchemaValidator {
    fn new(column_mapping_mode: ColumnMappingMode) -> Self {
        Self {
            cm_enabled: !matches!(column_mapping_mode, ColumnMappingMode::None),
            seen_paths: HashSet::new(),
            current_path: Vec::new(),
            errors: Vec::new(),
        }
    }

    fn into_result(self) -> DeltaResult<()> {
        if self.errors.is_empty() {
            Ok(())
        } else {
            Err(KernelError::generic(format!(
                "Schema validation failed:\n- {}",
                self.errors.join("\n- ")
            ))
            .into())
        }
    }
}

impl<'a> SchemaTransform<'a> for SchemaValidator {
    transform_output_type!(|'a, T| ());

    fn transform_struct_field(&mut self, field: &'a StructField) {
        if let Err(e) = validate_field_name(field.name(), self.cm_enabled) {
            self.errors.push(e.to_string());
        }

        // Check duplicate paths. We use a null-byte separator instead of dots because
        // column names can contain literal dots when column mapping is enabled. A dot
        // separator would make column "a.b" indistinguishable from nested field b in
        // struct a. Null bytes cannot appear in column names, so they are safe to use.
        self.current_path.push(field.name().to_ascii_lowercase());

        // Reject `delta.invariants` metadata on any field. Kernel cannot evaluate SQL
        // expression invariants, so reject at create time with a clear, path-aware error.
        //
        // Note: unlike `NonNullFieldChecker`, this validator intentionally does NOT
        // skip recursion into variant internals. Variants are not expected to carry
        // `delta.invariants`; if they ever do, bubble the error up loudly instead of
        // silently skipping it.
        //
        // When kernel gains SQL expression invariant support, remove this rejection
        // and replace it with a check that delegates to the invariant evaluation
        // pipeline.
        if field.has_invariants() {
            self.errors.push(format!(
                "Column '{}' has `delta.invariants` metadata; SQL expression invariants \
                 are not supported by kernel",
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
        return Err(KernelError::generic("Column name cannot be empty").into());
    }
    if cm_enabled {
        // Newlines break metadata serialization regardless of column mapping mode.
        if name.contains('\n') {
            return Err(KernelError::generic(format!(
                "Column name '{name}' contains a newline character, which is not allowed"
            ))
            .into());
        }
    } else if name.contains(INVALID_PARQUET_CHARS) {
        let invalid: Vec<char> = name
            .chars()
            .filter(|c| INVALID_PARQUET_CHARS.contains(c))
            .collect();
        return Err(KernelError::generic(format!(
            "Column name '{name}' contains invalid character(s) {invalid:?} that are not \
             allowed in Parquet column names. \
             Enable column mapping to use special characters in column names."
        ))
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::schema::{
        schema, ArrayType, ColumnMetadataKey, DataType, MetadataValue, StructField, StructType,
    };

    // === Schema builders for test cases ===

    fn simple_schema() -> StructType {
        schema! {
            not_null "id": INTEGER,
            nullable "name": STRING,
        }
    }

    fn schema_with_underscores() -> StructType {
        schema! {
            not_null "col_1": INTEGER,
            nullable "_private": STRING,
            not_null "CamelCase123": LONG,
        }
    }

    fn schema_with_special_chars() -> StructType {
        schema! {
            not_null "my column": INTEGER,
            nullable "col;name": STRING,
        }
    }

    fn schema_with_dot() -> StructType {
        schema! {
            not_null "a.b": INTEGER,
            nullable "c": STRING,
        }
    }

    fn schema_different_struct_children() -> StructType {
        schema! {
            not_null "a": { not_null "child": INTEGER },
            not_null "b": { nullable "CHILD": STRING },
        }
    }

    fn schema_with_space() -> StructType {
        schema! { not_null "my column": INTEGER }
    }

    fn schema_with_semicolon() -> StructType {
        schema! { not_null "col;name": INTEGER }
    }

    fn schema_with_newline() -> StructType {
        schema! { not_null "col\nname": INTEGER }
    }

    fn schema_with_empty_name() -> StructType {
        schema! { not_null "": INTEGER }
    }

    fn schema_nested_bad_char() -> StructType {
        schema! { not_null "parent": { not_null "bad column": INTEGER } }
    }

    fn schema_array_bad_char() -> StructType {
        schema! { not_null "arr": [ nullable { not_null "bad col": INTEGER } ] }
    }

    fn schema_map_bad_char() -> StructType {
        schema! { not_null "m": { STRING => nullable { not_null "bad;val": INTEGER } } }
    }

    fn schema_top_level_dup() -> StructType {
        let inner = StructType::new_unchecked(vec![StructField::not_null("x", DataType::INTEGER)]);
        StructType::new_unchecked(vec![
            StructField::not_null("a", inner),
            StructField::nullable("A", DataType::STRING),
        ])
    }

    fn schema_array_dup() -> StructType {
        let inner = StructType::new_unchecked(vec![
            StructField::not_null("x", DataType::INTEGER),
            StructField::nullable("X", DataType::STRING),
        ]);
        StructType::new_unchecked(vec![StructField::not_null(
            "arr",
            ArrayType::new(inner, true),
        )])
    }

    fn schema_multi_bad() -> StructType {
        schema! {
            not_null "good": INTEGER,
            nullable "bad column": STRING,
            not_null "col;name": LONG,
        }
    }

    // === Helpers for building invariants metadata ===
    //
    // These tests assert that `delta.invariants` metadata is rejected at CREATE TABLE.
    // When kernel gains SQL expression invariant support (see tracking issue for
    // invariant evaluation), these tests should be repurposed to feed a supported
    // invariant expression through the full write path instead of being deleted
    // outright.

    fn field_with_invariant(name: &str, data_type: DataType, nullable: bool) -> StructField {
        let mut field = StructField::new(name, data_type, nullable);
        field.metadata.insert(
            ColumnMetadataKey::Invariants.as_ref().to_string(),
            MetadataValue::String(r#"{"expression": {"expression": "x > 0"}}"#.to_string()),
        );
        field
    }

    fn schema_top_level_invariant() -> StructType {
        schema! {
            (field_with_invariant("x", DataType::INTEGER, true)),
            nullable "y": INTEGER,
        }
    }

    fn schema_nested_invariant() -> StructType {
        schema! {
            nullable "parent": {
                (field_with_invariant("child", DataType::INTEGER, true)),
            },
        }
    }

    fn schema_array_nested_invariant() -> StructType {
        schema! {
            nullable "arr": [ nullable {
                (field_with_invariant("child", DataType::INTEGER, true)),
            } ],
        }
    }

    fn schema_map_nested_invariant() -> StructType {
        schema! {
            nullable "map": { STRING => nullable {
                (field_with_invariant("child", DataType::INTEGER, true)),
            } },
        }
    }

    // === Valid schemas ===

    #[rstest]
    #[case::simple(simple_schema(), ColumnMappingMode::None)]
    #[case::underscores_digits(schema_with_underscores(), ColumnMappingMode::None)]
    #[case::special_chars_with_cm(schema_with_special_chars(), ColumnMappingMode::Name)]
    #[case::dot_in_name_with_cm(schema_with_dot(), ColumnMappingMode::Name)]
    #[case::different_struct_children(schema_different_struct_children(), ColumnMappingMode::None)]
    #[case::empty_no_cm(schema! {}, ColumnMappingMode::None)]
    #[case::empty_cm_name(schema! {}, ColumnMappingMode::Name)]
    #[case::empty_cm_id(schema! {}, ColumnMappingMode::Id)]
    fn valid_schema_accepted(#[case] schema: StructType, #[case] cm: ColumnMappingMode) {
        assert!(validate_schema(&schema, cm).is_ok());
    }

    // === Invalid schemas ===

    #[rstest]
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
        let result = validate_schema(&schema, cm);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        for expected in expected_errs {
            assert!(
                err.contains(expected),
                "Expected '{expected}' in error, got: {err}"
            );
        }
    }

    // === delta.invariants metadata rejection ===

    #[rstest]
    #[case::top_level(schema_top_level_invariant(), "x")]
    #[case::nested_struct(schema_nested_invariant(), "parent.child")]
    #[case::array_nested(schema_array_nested_invariant(), "arr.child")]
    #[case::map_nested(schema_map_nested_invariant(), "map.child")]
    fn invariants_metadata_rejected(#[case] schema: StructType, #[case] expected_path: &str) {
        let result = validate_schema(&schema, ColumnMappingMode::None);
        let err = result.expect_err("expected delta.invariants metadata rejection");
        let msg = err.to_string();
        assert!(
            msg.contains("delta.invariants"),
            "Expected delta.invariants mention in error, got: {msg}"
        );
        assert!(
            msg.contains(expected_path),
            "Expected path '{expected_path}' in error, got: {msg}"
        );
    }
}
