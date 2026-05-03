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
    container_depth: usize,
}

impl CheckVoidInComplexTypes {
    /// Recurses into a container element/value type, tracking that we are inside an Array or Map
    /// so that nested struct fields with void type can be rejected.
    fn descend_into_container<'a>(&mut self, etype: &'a DataType) -> Option<Cow<'a, DataType>> {
        self.container_depth += 1;
        let result = self.transform(etype);
        self.container_depth -= 1;
        result
    }
}

impl<'a> SchemaTransform<'a> for CheckVoidInComplexTypes {
    transform_output_type!(|'a, T| Option<Cow<'a, T>>);

    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        if self.error_message.is_some() {
            return Some(Cow::Borrowed(field));
        }
        if self.container_depth > 0 && *field.data_type() == DataType::VOID {
            self.error_message =
                Some("Void type is not allowed inside a struct nested in Array or Map");
            return Some(Cow::Borrowed(field));
        }
        self.recurse_into_struct_field(field)
    }

    fn transform_array_element(&mut self, etype: &'a DataType) -> Option<Cow<'a, DataType>> {
        if *etype == DataType::VOID {
            self.error_message = Some("Void type is not allowed as an array element type");
            return Some(Cow::Borrowed(etype));
        }
        self.descend_into_container(etype)
    }

    fn transform_map_key(&mut self, etype: &'a DataType) -> Option<Cow<'a, DataType>> {
        if *etype == DataType::VOID {
            self.error_message = Some("Void type is not allowed as a map key type");
            return Some(Cow::Borrowed(etype));
        }
        self.descend_into_container(etype)
    }

    fn transform_map_value(&mut self, etype: &'a DataType) -> Option<Cow<'a, DataType>> {
        if *etype == DataType::VOID {
            self.error_message = Some("Void type is not allowed as a map value type");
            return Some(Cow::Borrowed(etype));
        }
        self.descend_into_container(etype)
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
        transform_output_type!(|'a, T| Option<Cow<'a, T>>);

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
/// - Void is nested inside Array or Map. Parquet's UNKNOWN logical type can in principle annotate
///   any physical type with all-null values, but Delta itself does not materialize void columns in
///   data files, and our logical-to-physical transform does not descend into Array elements or Map
///   values to drop them.
/// - A struct has only void fields (would produce an empty Parquet struct)
/// - All top-level columns are void (would produce an empty Parquet schema)
pub(crate) fn validate_schema_for_write(schema: &Schema) -> DeltaResult<()> {
    struct CheckAllVoidStructs {
        error_message: Option<&'static str>,
    }

    impl<'a> SchemaTransform<'a> for CheckAllVoidStructs {
        transform_output_type!(|'a, T| Option<Cow<'a, T>>);

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
    use crate::schema::{
        ArrayType, ColumnMetadataKey, DataType, MapType, MetadataValue, StructField, StructType,
    };

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
    #[case(
        "void in array inside struct",
        StructField::nullable(
            "outer",
            DataType::Struct(Box::new(StructType::new_unchecked([
                StructField::nullable(
                    "inner",
                    DataType::Array(Box::new(ArrayType::new(DataType::VOID, true))),
                ),
            ])))
        ),
        "array element type"
    )]
    #[case(
        "void in map inside array",
        StructField::nullable(
            "col",
            DataType::Array(Box::new(ArrayType::new(
                DataType::Map(Box::new(MapType::new(DataType::STRING, DataType::VOID, true,))),
                true,
            ))),
        ),
        "map value type"
    )]
    #[case(
        "void inside struct nested in array",
        StructField::nullable(
            "arr",
            DataType::Array(Box::new(ArrayType::new(
                DataType::Struct(Box::new(StructType::new_unchecked([
                    StructField::nullable("a", DataType::INTEGER),
                    StructField::nullable("b", DataType::VOID),
                ]))),
                true,
            ))),
        ),
        "struct nested in Array or Map"
    )]
    #[case(
        "void inside struct nested in map value",
        StructField::nullable(
            "m",
            DataType::Map(Box::new(MapType::new(
                DataType::STRING,
                DataType::Struct(Box::new(StructType::new_unchecked([
                    StructField::nullable("a", DataType::INTEGER),
                    StructField::nullable("b", DataType::VOID),
                ]))),
                true,
            ))),
        ),
        "struct nested in Array or Map"
    )]
    #[case(
        "void inside struct nested in map key",
        StructField::nullable(
            "m",
            DataType::Map(Box::new(MapType::new(
                DataType::Struct(Box::new(StructType::new_unchecked([
                    StructField::nullable("a", DataType::INTEGER),
                    StructField::nullable("b", DataType::VOID),
                ]))),
                DataType::STRING,
                true,
            ))),
        ),
        "struct nested in Array or Map"
    )]
    #[case(
        "void inside struct nested in array inside array",
        StructField::nullable(
            "outer",
            DataType::Array(Box::new(ArrayType::new(
                DataType::Array(Box::new(ArrayType::new(
                    DataType::Struct(Box::new(StructType::new_unchecked([
                        StructField::nullable("a", DataType::INTEGER),
                        StructField::nullable("b", DataType::VOID),
                    ]))),
                    true,
                ))),
                true,
            ))),
        ),
        "struct nested in Array or Map"
    )]
    #[case(
        "void in deeply nested struct inside array",
        StructField::nullable(
            "arr",
            DataType::Array(Box::new(ArrayType::new(
                DataType::Struct(Box::new(StructType::new_unchecked([
                    StructField::nullable("a", DataType::INTEGER),
                    StructField::nullable(
                        "b",
                        DataType::Struct(Box::new(StructType::new_unchecked([
                            StructField::nullable("x", DataType::INTEGER),
                            StructField::nullable("y", DataType::VOID),
                        ]))),
                    ),
                ]))),
                true,
            ))),
        ),
        "struct nested in Array or Map"
    )]
    #[case(
        "void in struct inside array inside struct inside array",
        StructField::nullable(
            "outer",
            DataType::Array(Box::new(ArrayType::new(
                DataType::Struct(Box::new(StructType::new_unchecked([StructField::nullable(
                    "inner",
                    DataType::Array(Box::new(ArrayType::new(
                        DataType::Struct(Box::new(StructType::new_unchecked([
                            StructField::nullable("v", DataType::VOID),
                        ]))),
                        true,
                    ))),
                )]))),
                true,
            ))),
        ),
        "struct nested in Array or Map"
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

    #[rstest::rstest]
    #[case(
        "void top level ok",
        StructType::new_unchecked([
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable("void_col", DataType::VOID),
        ])
    )]
    #[case(
        "test no void ok",
        StructType::new_unchecked([
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable("name", DataType::STRING),
        ])
    )]
    #[case(
        "void in nested struct",
        StructType::new_unchecked([StructField::nullable(
            "s",
            DataType::Struct(Box::new(StructType::new_unchecked([
                StructField::nullable("a", DataType::INTEGER),
                StructField::nullable("b", DataType::VOID),
            ]))),
        )])
    )]
    #[case(
        "array of struct without void",
        StructType::new_unchecked([StructField::nullable(
            "arr",
            DataType::Array(Box::new(ArrayType::new(
                DataType::Struct(Box::new(StructType::new_unchecked([
                    StructField::nullable("a", DataType::INTEGER),
                    StructField::nullable("b", DataType::STRING),
                ]))),
                true,
            ))),
        )])
    )]
    #[case(
        "map of struct without void",
        StructType::new_unchecked([StructField::nullable(
            "m",
            DataType::Map(Box::new(MapType::new(
                DataType::STRING,
                DataType::Struct(Box::new(StructType::new_unchecked([
                    StructField::nullable("a", DataType::INTEGER),
                    StructField::nullable("b", DataType::STRING),
                ]))),
                true,
            ))),
        )])
    )]
    fn test_valid_schema_for_complex_types(#[case] desc: &str, #[case] schema: StructType) {
        validate_no_void_in_complex_types(&schema)
            .unwrap_or_else(|e| panic!("{desc}: unexpected validation error: {e}"));
    }

    // ---- validate_schema_for_write tests ----

    #[rstest::rstest]
    #[case(
        "with void column",
        StructType::new_unchecked([
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable("void_col", DataType::VOID),
        ])
    )]
    #[case(
        "no void",
        StructType::new_unchecked([
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable("name", DataType::STRING),
        ])
    )]
    #[case(
        "struct with mixed void",
        StructType::new_unchecked([StructField::nullable(
            "s",
            DataType::Struct(Box::new(StructType::new_unchecked([
                StructField::nullable("a", DataType::INTEGER),
                StructField::nullable("b", DataType::VOID),
            ]))),
        )])
    )]
    fn test_write_valid_schemas(#[case] desc: &str, #[case] schema: StructType) {
        validate_schema_for_write(&schema)
            .unwrap_or_else(|e| panic!("{desc}: unexpected validation error: {e}"));
    }

    #[rstest::rstest]
    #[case(
        "all void table",
        StructType::new_unchecked([
            StructField::nullable("a", DataType::VOID),
            StructField::nullable("b", DataType::VOID),
        ]),
        "all fields are void"
    )]
    #[case(
        "all void struct",
        StructType::new_unchecked([
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable(
                "s",
                DataType::Struct(Box::new(StructType::new_unchecked([
                    StructField::nullable("x", DataType::VOID),
                    StructField::nullable("y", DataType::VOID),
                ]))),
            ),
        ]),
        "all fields are void"
    )]
    #[case(
        "void in array",
        StructType::new_unchecked([StructField::nullable(
            "arr",
            DataType::Array(Box::new(ArrayType::new(DataType::VOID, true))),
        )]),
        "array element type"
    )]
    #[case(
        "void in map",
        StructType::new_unchecked([StructField::nullable(
            "m",
            DataType::Map(Box::new(MapType::new(
                DataType::STRING,
                DataType::VOID,
                true,
            ))),
        )]),
        "map value type"
    )]
    #[case(
        "nested all void struct",
        StructType::new_unchecked([
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
        ]),
        "all fields are void"
    )]
    fn test_write_rejected_schemas(
        #[case] desc: &str,
        #[case] schema: StructType,
        #[case] expected_msg: &str,
    ) {
        let result = validate_schema_for_write(&schema);
        assert!(
            result.unwrap_err().to_string().contains(expected_msg),
            "{desc}: expected error containing '{expected_msg}'"
        );
    }

    // ---- strip_void_from_field tests ----

    #[rstest::rstest]
    #[case(
        "non-struct passthrough",
        StructField::nullable("x", DataType::INTEGER),
        StructField::nullable("x", DataType::INTEGER)
    )]
    #[case(
        "struct with mixed void",
        StructField::nullable(
            "s",
            DataType::Struct(Box::new(StructType::new_unchecked([
                StructField::nullable("a", DataType::INTEGER),
                StructField::nullable("b", DataType::VOID),
                StructField::nullable("c", DataType::STRING),
            ]))),
        ),
        StructField::nullable(
            "s",
            DataType::Struct(Box::new(StructType::new_unchecked([
                StructField::nullable("a", DataType::INTEGER),
                StructField::nullable("c", DataType::STRING),
            ]))),
        )
    )]
    #[case(
        "deeply nested void",
        StructField::nullable(
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
        ),
        StructField::nullable(
            "outer",
            DataType::Struct(Box::new(StructType::new_unchecked([
                StructField::nullable(
                    "inner",
                    DataType::Struct(Box::new(StructType::new_unchecked([
                        StructField::nullable("a", DataType::INTEGER),
                    ]))),
                ),
            ]))),
        )
    )]
    #[case(
        "no void is noop",
        StructField::nullable(
            "s",
            DataType::Struct(Box::new(StructType::new_unchecked([
                StructField::nullable("a", DataType::INTEGER),
                StructField::nullable("b", DataType::STRING),
            ]))),
        ),
        StructField::nullable(
            "s",
            DataType::Struct(Box::new(StructType::new_unchecked([
                StructField::nullable("a", DataType::INTEGER),
                StructField::nullable("b", DataType::STRING),
            ]))),
        )
    )]
    fn test_strip_void_from_field(
        #[case] desc: &str,
        #[case] input: StructField,
        #[case] expected: StructField,
    ) {
        let stripped = strip_void_from_field(&input);
        assert_eq!(stripped, expected, "{desc}");
    }

    #[test]
    fn test_strip_preserves_metadata() {
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
}
