//! Write-time validation for void type usage in schemas.
//!
//! The Delta protocol allows void columns in table metadata. Void columns are never written to
//! Parquet files; reads generate null values on the fly for missing void columns. However, certain
//! void placements make data writes impossible and must be rejected at write time:
//! - Void nested inside Array or Map types (cannot represent in Parquet)
//! - Structs where all fields are void (empty Parquet struct)
//! - Tables where all fields are void (empty Parquet schema)

use std::borrow::Cow;

use super::{DataType, Schema, StructField, StructType};
use crate::transforms::{transform_output_type, SchemaTransform};
use crate::{DeltaResult, Error};

/// Schema visitor that detects void type nested inside Array or Map.
#[derive(Debug, Default)]
struct CheckVoidInComplexTypes {
    error_message: Option<&'static str>,
}

impl<'a> SchemaTransform<'a> for CheckVoidInComplexTypes {
    transform_output_type!(|'a, T| Option<Cow<'a, T>>);

    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        if self.error_message.is_some() {
            return Some(Cow::Borrowed(field));
        }
        self.recurse_into_struct_field(field)
    }

    fn transform_array_element(&mut self, etype: &'a DataType) -> Option<Cow<'a, DataType>> {
        if *etype == DataType::VOID {
            self.error_message = Some("Void type is not allowed as an array element type");
            return Some(Cow::Borrowed(etype));
        }
        self.transform(etype)
    }

    fn transform_map_key(&mut self, etype: &'a DataType) -> Option<Cow<'a, DataType>> {
        if *etype == DataType::VOID {
            self.error_message = Some("Void type is not allowed as a map key type");
            return Some(Cow::Borrowed(etype));
        }
        self.transform(etype)
    }

    fn transform_map_value(&mut self, etype: &'a DataType) -> Option<Cow<'a, DataType>> {
        if *etype == DataType::VOID {
            self.error_message = Some("Void type is not allowed as a map value type");
            return Some(Cow::Borrowed(etype));
        }
        self.transform(etype)
    }
}

/// Validates that a schema does not contain void type nested inside Array or Map.
fn validate_no_void_in_complex_types(schema: &Schema) -> DeltaResult<()> {
    let mut checker = CheckVoidInComplexTypes::default();
    let _ = checker.transform_struct(schema);
    if let Some(msg) = checker.error_message {
        return Err(Error::schema(msg));
    }
    Ok(())
}

/// Returns true if a struct has fields and ALL of them are void (recursively treating
/// all-void nested structs as void-equivalent).
fn is_all_void_struct(st: &StructType) -> bool {
    st.num_fields() > 0
        && st.fields().all(|f| {
            *f.data_type() == DataType::VOID
                || matches!(f.data_type(), DataType::Struct(inner) if is_all_void_struct(inner))
        })
}

/// Recursively strips void fields from a struct field. If the field's data type is a struct,
/// void sub-fields are removed. Non-struct fields are returned as-is.
pub(crate) fn strip_void_from_field(field: &StructField) -> StructField {
    struct StripVoidFields;

    impl<'a> SchemaTransform<'a> for StripVoidFields {
        fn transform_struct_field(
            &mut self,
            field: &'a StructField,
        ) -> Option<Cow<'a, StructField>> {
            if *field.data_type() == DataType::VOID {
                return None;
            }
            self.recurse_into_struct_field(field)
        }
    }

    let mut transformer = StripVoidFields;
    transformer
        .recurse_into_struct_field(field)
        .map(|cow| cow.into_owned())
        .unwrap_or_else(|| field.clone())
}

/// Validates that a schema is suitable for writing data. Writes are rejected when:
/// - Void is nested inside Array or Map (not written to Parquet by Delta)
/// - A struct has only void fields (would produce an empty Parquet struct)
/// - All top-level columns are void (would produce an empty Parquet schema)
///
/// These checks are NOT applied at read time or for metadata-only operations.
pub(crate) fn validate_schema_for_write(schema: &Schema) -> DeltaResult<()> {
    struct CheckAllVoidStructs {
        error_message: Option<&'static str>,
    }

    impl<'a> SchemaTransform<'a> for CheckAllVoidStructs {
        fn transform_struct(&mut self, stype: &'a StructType) -> Option<Cow<'a, StructType>> {
            if self.error_message.is_some() {
                return Some(Cow::Borrowed(stype));
            }
            if is_all_void_struct(stype) {
                self.error_message =
                    Some("Cannot write to a table with a struct where all fields are void");
                return Some(Cow::Borrowed(stype));
            }
            self.recurse_into_struct(stype)
        }
    }

    validate_no_void_in_complex_types(schema)?;

    let mut checker = CheckAllVoidStructs {
        error_message: None,
    };

    let _ = checker.transform_struct(schema);
    if let Some(msg) = checker.error_message {
        return Err(Error::schema(msg));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ArrayType, DataType, MapType, StructField, StructType};

    // ---- validate_no_void_in_complex_types tests ----

    /// Demonstrates that serde deserialization bypasses `MapType::new`, so adding
    /// validation to the constructor would not catch void-in-map from Delta metadata.
    #[test]
    fn test_serde_bypasses_map_constructor() {
        let json = r#"{
            "name": "m",
            "type": {
                "type": "map",
                "keyType": "string",
                "valueType": "void",
                "valueContainsNull": true
            },
            "nullable": true,
            "metadata": {}
        }"#;

        // Deserialization succeeds — serde populates fields directly
        let field: StructField = serde_json::from_str(json).unwrap();
        if let DataType::Map(map_type) = field.data_type() {
            assert_eq!(*map_type.value_type(), DataType::VOID);
        } else {
            panic!("expected map type");
        }

        // The dedicated validator is what actually catches this
        let schema = StructType::new_unchecked([field]);
        assert!(validate_no_void_in_complex_types(&schema).is_err());
    }

    #[rstest::rstest]
    #[case(
        "void in array",
        StructField::nullable(
            "f",
            DataType::Array(Box::new(ArrayType::new(DataType::VOID, true)))
        ),
        "array element type"
    )]
    #[case(
        "void in map value",
        StructField::nullable(
            "f",
            DataType::Map(Box::new(MapType::new(DataType::STRING, DataType::VOID, true)))
        ),
        "map value type"
    )]
    #[case(
        "void in map key",
        StructField::nullable(
            "f",
            DataType::Map(Box::new(MapType::new(DataType::VOID, DataType::STRING, true)))
        ),
        "map key type"
    )]
    fn test_void_in_complex_type_rejected(
        #[case] desc: &str,
        #[case] field: StructField,
        #[case] expected_msg: &str,
    ) {
        let schema = StructType::new_unchecked([field]);
        let result = validate_no_void_in_complex_types(&schema);
        assert!(
            result.unwrap_err().to_string().contains(expected_msg),
            "{desc}: expected error containing '{expected_msg}'"
        );
    }

    #[test]
    fn test_void_top_level_ok() {
        let schema = StructType::new_unchecked([
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable("void_col", DataType::VOID),
        ]);
        validate_no_void_in_complex_types(&schema).expect("Top-level void should be allowed");
    }

    #[test]
    fn test_void_in_array_inside_struct_rejected() {
        let schema = StructType::new_unchecked([StructField::nullable(
            "outer",
            DataType::Struct(Box::new(StructType::new_unchecked([
                StructField::nullable(
                    "inner",
                    DataType::Array(Box::new(ArrayType::new(DataType::VOID, true))),
                ),
            ]))),
        )]);
        let result = validate_no_void_in_complex_types(&schema);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("array element type"));
    }

    #[test]
    fn test_void_in_map_inside_array_rejected() {
        let schema = StructType::new_unchecked([StructField::nullable(
            "col",
            DataType::Array(Box::new(ArrayType::new(
                DataType::Map(Box::new(MapType::new(
                    DataType::STRING,
                    DataType::VOID,
                    true,
                ))),
                true,
            ))),
        )]);
        let result = validate_no_void_in_complex_types(&schema);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("map value type"));
    }

    #[test]
    fn test_no_void_ok() {
        let schema = StructType::new_unchecked([
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable("name", DataType::STRING),
        ]);
        validate_no_void_in_complex_types(&schema).expect("Schema without void should be fine");
    }

    // ---- validate_schema_for_write tests ----

    #[test]
    fn test_write_ok_with_void_column() {
        let schema = StructType::new_unchecked([
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable("void_col", DataType::VOID),
        ]);
        validate_schema_for_write(&schema)
            .expect("Table with some void columns should be writable");
    }

    #[test]
    fn test_write_rejects_all_void_table() {
        let schema = StructType::new_unchecked([
            StructField::nullable("a", DataType::VOID),
            StructField::nullable("b", DataType::VOID),
        ]);
        let result = validate_schema_for_write(&schema);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("all fields are void"));
    }

    #[test]
    fn test_write_rejects_all_void_struct() {
        let schema = StructType::new_unchecked([
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable(
                "s",
                DataType::Struct(Box::new(StructType::new_unchecked([
                    StructField::nullable("x", DataType::VOID),
                    StructField::nullable("y", DataType::VOID),
                ]))),
            ),
        ]);
        let result = validate_schema_for_write(&schema);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("all fields are void"));
    }

    #[test]
    fn test_write_rejects_void_in_array() {
        let schema = StructType::new_unchecked([StructField::nullable(
            "arr",
            DataType::Array(Box::new(ArrayType::new(DataType::VOID, true))),
        )]);
        let result = validate_schema_for_write(&schema);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("array element type"));
    }

    #[test]
    fn test_write_rejects_void_in_map() {
        let schema = StructType::new_unchecked([StructField::nullable(
            "m",
            DataType::Map(Box::new(MapType::new(
                DataType::STRING,
                DataType::VOID,
                true,
            ))),
        )]);
        let result = validate_schema_for_write(&schema);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("map value type"));
    }

    #[test]
    fn test_write_ok_no_void() {
        let schema = StructType::new_unchecked([
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable("name", DataType::STRING),
        ]);
        validate_schema_for_write(&schema).expect("No void columns should be fine");
    }

    #[test]
    fn test_write_ok_struct_with_mixed_void() {
        // struct<a: int, b: void> is writable (not all-void)
        let schema = StructType::new_unchecked([StructField::nullable(
            "s",
            DataType::Struct(Box::new(StructType::new_unchecked([
                StructField::nullable("a", DataType::INTEGER),
                StructField::nullable("b", DataType::VOID),
            ]))),
        )]);
        validate_schema_for_write(&schema).expect("Struct with mixed fields should be writable");
    }

    #[test]
    fn test_write_rejects_nested_all_void_struct() {
        // {id: int, outer: struct<inner: struct<x: void>>} — inner is all-void
        let schema = StructType::new_unchecked([
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable(
                "outer",
                DataType::Struct(Box::new(StructType::new_unchecked([
                    StructField::nullable(
                        "inner",
                        DataType::Struct(Box::new(StructType::new_unchecked([
                            StructField::nullable("x", DataType::VOID),
                        ]))),
                    ),
                ]))),
            ),
        ]);
        let result = validate_schema_for_write(&schema);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("all fields are void"));
    }

    // ---- strip_void_from_field tests ----

    #[test]
    fn test_strip_non_struct_is_passthrough() {
        let field = StructField::nullable("x", DataType::INTEGER);
        let stripped = strip_void_from_field(&field);
        assert_eq!(stripped.name(), "x");
        assert_eq!(*stripped.data_type(), DataType::INTEGER);
    }

    #[test]
    fn test_strip_struct_with_mixed_void() {
        let field = StructField::nullable(
            "s",
            DataType::Struct(Box::new(StructType::new_unchecked([
                StructField::nullable("a", DataType::INTEGER),
                StructField::nullable("b", DataType::VOID),
                StructField::nullable("c", DataType::STRING),
            ]))),
        );
        let stripped = strip_void_from_field(&field);
        if let DataType::Struct(inner) = stripped.data_type() {
            assert_eq!(inner.fields().count(), 2);
            assert!(inner.field("a").is_some());
            assert!(inner.field("b").is_none(), "void field should be removed");
            assert!(inner.field("c").is_some());
        } else {
            panic!("expected struct");
        }
    }

    #[test]
    fn test_strip_deeply_nested_void() {
        // struct<outer: struct<inner: struct<a: int, v: void>>>
        let field = StructField::nullable(
            "outer",
            DataType::Struct(Box::new(StructType::new_unchecked([
                StructField::nullable(
                    "inner",
                    DataType::Struct(Box::new(StructType::new_unchecked([
                        StructField::nullable("a", DataType::INTEGER),
                        StructField::nullable("v", DataType::VOID),
                    ]))),
                ),
            ]))),
        );
        let stripped = strip_void_from_field(&field);
        if let DataType::Struct(outer) = stripped.data_type() {
            let inner_field = outer.field("inner").expect("inner should exist");
            if let DataType::Struct(inner) = inner_field.data_type() {
                assert_eq!(inner.fields().count(), 1);
                assert!(inner.field("a").is_some());
                assert!(
                    inner.field("v").is_none(),
                    "void should be stripped at depth 3"
                );
            } else {
                panic!("expected struct");
            }
        } else {
            panic!("expected struct");
        }
    }

    #[test]
    fn test_strip_preserves_metadata() {
        use crate::schema::{ColumnMetadataKey, MetadataValue};
        let mut field = StructField::nullable(
            "s",
            DataType::Struct(Box::new(StructType::new_unchecked([
                StructField::nullable("a", DataType::INTEGER),
                StructField::nullable("b", DataType::VOID),
            ]))),
        );
        field.metadata.insert(
            ColumnMetadataKey::ColumnMappingPhysicalName.as_ref().into(),
            MetadataValue::String("phys_s".into()),
        );
        let stripped = strip_void_from_field(&field);
        assert_eq!(
            stripped
                .metadata
                .get(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref()),
            Some(&MetadataValue::String("phys_s".into())),
            "metadata should be preserved after stripping"
        );
    }

    #[test]
    fn test_strip_no_void_is_noop() {
        let field = StructField::nullable(
            "s",
            DataType::Struct(Box::new(StructType::new_unchecked([
                StructField::nullable("a", DataType::INTEGER),
                StructField::nullable("b", DataType::STRING),
            ]))),
        );
        let stripped = strip_void_from_field(&field);
        if let DataType::Struct(inner) = stripped.data_type() {
            assert_eq!(inner.fields().count(), 2);
        } else {
            panic!("expected struct");
        }
    }
}
