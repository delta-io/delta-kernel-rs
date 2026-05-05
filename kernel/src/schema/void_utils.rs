//! Write-time validation for void type usage in schemas.
//!
//! The Delta protocol allows void columns in table metadata. Void columns are never written to
//! Parquet files; reads generate null values on the fly for missing void columns. However, certain
//! void placements make data writes impossible and must be rejected at write time:
//! - Void nested inside Array or Map types (not materialized by Delta, and not supported by the
//!   logical-to-physical write transform, which only descends through Struct fields)
//! - Structs that contain no non-void fields (would produce an empty Parquet struct)
//! - Tables that contain no non-void columns (would produce an empty Parquet schema)

use std::borrow::Cow;
use std::sync::Arc;

use super::{DataType, PrimitiveType, Schema, SchemaRef, StructField, StructType};
use crate::expressions::Transform;
use crate::transforms::{transform_output_type, SchemaTransform};
use crate::{DeltaResult, Error, Expression};

/// Returns true when this struct directly contains no non-void fields. The check is local --
/// it does not recurse into nested structs, because the caller (`ValidateForWrite`) walks every
/// struct in the schema and applies this predicate at each level. Empty structs also qualify
/// (they would produce an unwriteable empty Parquet struct).
///
/// This predicate is the validator's responsibility, not the stripper's. The stripper could
/// derive a reduced physical schema for an all-void struct, but write semantics require
/// rejecting the schema before that derivation happens.
fn has_no_non_void_fields(st: &StructType) -> bool {
    st.fields().all(|f| *f.data_type() == DataType::VOID)
}

/// Schema visitor that drops void fields at every nesting level.
///
/// This is intentionally separate from `ValidateForWrite`: validation decides
/// whether a logical write schema is allowed, while this transform derives the
/// physical schema used for Parquet writes, including metadata-only paths.
struct StripVoidFields;

impl<'a> SchemaTransform<'a> for StripVoidFields {
    transform_output_type!(|'a, T| Option<Cow<'a, T>>);

    fn transform_primitive(&mut self, ptype: &'a PrimitiveType) -> Option<Cow<'a, PrimitiveType>> {
        (*ptype != PrimitiveType::Void).then_some(Cow::Borrowed(ptype))
    }
}

/// Returns `schema` with all void fields removed from structs at every nesting level.
/// In the common case where the schema contains no void fields, returns the same `Arc`
/// without copying.
///
/// Pairs with `validate_schema_for_write`, which rejects void placements inside Array or Map
/// before the stripped physical schema is used for writing. An all-void or empty input yields
/// an empty schema; write callers are expected to run `validate_schema_for_write` before using
/// the stripped schema.
pub(crate) fn strip_void_from_schema(schema: SchemaRef) -> SchemaRef {
    match StripVoidFields.transform_struct(&schema) {
        Some(Cow::Owned(stripped)) => Arc::new(stripped),
        Some(Cow::Borrowed(_)) => schema,
        None => Arc::new(StructType::new_unchecked(Vec::<StructField>::new())),
    }
}

/// Validates that a schema is suitable for writing data. This is the kernel-internal write-time
/// rejection point for invalid void placements: [`StructType::try_new`] validates structural
/// properties only (field-name uniqueness, metadata-column rules) and accepts schemas like
/// `Array<Void>` or all-void structs. Both JSON-deserialized metadata (which round-trips through
/// `try_new`) and any `new_unchecked` paths therefore rely on this validator.
///
/// Writes are rejected when:
/// - Void is nested inside Array or Map. Parquet's UNKNOWN logical type can in principle annotate
///   any physical type with all-null values, but Delta itself does not materialize void columns in
///   data files, and our logical-to-physical transform does not descend into Array elements or Map
///   values to drop them.
/// - A struct contains no non-void fields (would produce an empty Parquet struct)
/// - The table schema contains no non-void columns (would produce an empty Parquet schema)
pub(crate) fn validate_schema_for_write(schema: &Schema) -> DeltaResult<()> {
    ValidateForWrite {
        container_depth: 0,
        depth: 0,
    }
    .transform_struct(schema)
}

struct ValidateForWrite {
    container_depth: usize,
    depth: usize,
}

impl ValidateForWrite {
    fn descend_into_container(&mut self, etype: &DataType, position: &str) -> DeltaResult<()> {
        if *etype == DataType::VOID {
            return Err(Error::schema(format!(
                "Void type is not allowed as {position}"
            )));
        }
        self.container_depth += 1;
        let result = self.transform(etype);
        self.container_depth -= 1;
        result
    }
}

impl<'a> SchemaTransform<'a> for ValidateForWrite {
    transform_output_type!(|'a, T| DeltaResult<()>);

    fn transform_struct(&mut self, stype: &'a StructType) -> DeltaResult<()> {
        if has_no_non_void_fields(stype) {
            return Err(Error::schema(if self.container_depth > 0 {
                "A struct nested in Array or Map must contain at least one non-void field"
            } else if self.depth == 0 {
                "Table schema must contain at least one non-void column"
            } else {
                "Cannot write to a table with a struct that contains no non-void fields"
            }));
        }
        self.depth += 1;
        let result = self.recurse_into_struct(stype);
        self.depth -= 1;
        result
    }

    fn transform_struct_field(&mut self, field: &'a StructField) -> DeltaResult<()> {
        if self.container_depth > 0 && *field.data_type() == DataType::VOID {
            return Err(Error::schema(
                "Void type is not allowed inside a struct nested in Array or Map",
            ));
        }
        self.recurse_into_struct_field(field)
    }

    fn transform_array_element(&mut self, etype: &'a DataType) -> DeltaResult<()> {
        self.descend_into_container(etype, "an array element type")
    }

    fn transform_map_key(&mut self, etype: &'a DataType) -> DeltaResult<()> {
        self.descend_into_container(etype, "a map key type")
    }

    fn transform_map_value(&mut self, etype: &'a DataType) -> DeltaResult<()> {
        self.descend_into_container(etype, "a map value type")
    }
}

/// Appends void-field-stripping operations to `transform`, recursing through struct fields in
/// `st` (the logical schema). Returns `transform` unchanged when `st` contains no void fields.
///
/// Composition: `st` is read directly to locate void fields, not the partial result of applying
/// `transform`. Composing `transform` first with operations on disjoint columns (e.g. partition
/// column drops) is therefore safe, but composing with operations that already drop or replace
/// the same void-named columns is not -- the resulting transform would double-drop or conflict.
/// Don't reorder this relative to such operations.
pub(crate) fn add_void_stripping(
    transform: Transform,
    st: &StructType,
    path: &[&str],
) -> Transform {
    add_void_stripping_inner(transform, st, path).0
}

/// Single-pass helper that builds the void-stripping transform and reports whether any field
/// was actually rewritten. The caller composes child results without a separate `contains_void`
/// pre-check, avoiding the O(depth^2) walk a naive double-traversal would produce.
///
/// This intentionally descends only through Struct fields; write validation rejects void
/// placements inside Array or Map before this transform is used.
fn add_void_stripping_inner(
    mut transform: Transform,
    st: &StructType,
    path: &[&str],
) -> (Transform, bool) {
    let mut changed = false;
    for field in st.fields() {
        if *field.data_type() == DataType::VOID {
            transform = transform.with_dropped_field(field.name());
            changed = true;
        } else if let DataType::Struct(inner) = field.data_type() {
            let mut child_path: Vec<&str> = path.to_vec();
            child_path.push(field.name());
            let (nested, nested_changed) = add_void_stripping_inner(
                Transform::new_nested(child_path.iter().copied()),
                inner,
                &child_path,
            );
            if nested_changed {
                let nested = Arc::new(Expression::transform(nested));
                transform = transform.with_replaced_field(field.name(), nested);
                changed = true;
            }
        }
    }
    (transform, changed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{
        ArrayType, ColumnMetadataKey, DataType, MapType, MetadataValue, StructField, StructType,
    };

    // ---- validate_schema_for_write tests ----

    #[test]
    fn test_validator_catches_void_in_map_from_json() {
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
        assert!(validate_schema_for_write(&schema).is_err());
    }

    #[rstest::rstest]
    #[case(
        "void in array",
        StructField::nullable("f", ArrayType::new(DataType::VOID, true)),
        "array element type"
    )]
    #[case(
        "void in map value",
        StructField::nullable("f", MapType::new(DataType::STRING, DataType::VOID, true)),
        "map value type"
    )]
    #[case(
        "void in map key",
        StructField::nullable("f", MapType::new(DataType::VOID, DataType::STRING, true)),
        "map key type"
    )]
    #[case(
        "void in array inside struct",
        StructField::nullable(
            "outer",
            StructType::new_unchecked([
                StructField::nullable(
                    "inner",
                    DataType::Array(Box::new(ArrayType::new(DataType::VOID, true))),
                ),
            ])
        ),
        "array element type"
    )]
    #[case(
        "void in map inside array",
        StructField::nullable(
            "col",
            ArrayType::new(
                DataType::Map(Box::new(MapType::new(DataType::STRING, DataType::VOID, true,))),
                true,
            ),
        ),
        "map value type"
    )]
    #[case(
        "void inside struct nested in array",
        StructField::nullable(
            "arr",
            ArrayType::new(
                DataType::Struct(Box::new(StructType::new_unchecked([
                    StructField::nullable("a", DataType::INTEGER),
                    StructField::nullable("b", DataType::VOID),
                ]))),
                true,
            ),
        ),
        "Void type is not allowed inside"
    )]
    #[case(
        "void inside struct nested in map value",
        StructField::nullable(
            "m",
            MapType::new(
                DataType::STRING,
                StructType::new_unchecked([
                    StructField::nullable("a", DataType::INTEGER),
                    StructField::nullable("b", DataType::VOID),
                ]),
                true,
            ),
        ),
        "Void type is not allowed inside"
    )]
    #[case(
        "void inside struct nested in map key",
        StructField::nullable(
            "m",
            MapType::new(
                StructType::new_unchecked([
                    StructField::nullable("a", DataType::INTEGER),
                    StructField::nullable("b", DataType::VOID),
                ]),
                DataType::STRING,
                true,
            ),
        ),
        "Void type is not allowed inside"
    )]
    #[case(
        "void inside struct nested in array inside array",
        StructField::nullable(
            "outer",
            ArrayType::new(
                DataType::Array(Box::new(ArrayType::new(
                    DataType::Struct(Box::new(StructType::new_unchecked([
                        StructField::nullable("a", DataType::INTEGER),
                        StructField::nullable("b", DataType::VOID),
                    ]))),
                    true,
                ))),
                true,
            ),
        ),
        "Void type is not allowed inside"
    )]
    #[case(
        "void in deeply nested struct inside array",
        StructField::nullable(
            "arr",
            ArrayType::new(
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
            ),
        ),
        "Void type is not allowed inside"
    )]
    #[case(
        "void in struct inside array inside struct inside array",
        StructField::nullable(
            "outer",
            ArrayType::new(
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
            ),
        ),
        "must contain at least one non-void field"
    )]
    #[case(
        "empty struct nested in array",
        StructField::nullable(
            "arr",
            ArrayType::new(
                DataType::Struct(Box::new(StructType::new_unchecked(
                    Vec::<StructField>::new(),
                ))),
                true,
            ),
        ),
        "struct nested in Array or Map must contain at least one non-void field"
    )]
    #[case(
        "all-void struct nested in map value",
        StructField::nullable(
            "m",
            MapType::new(
                DataType::STRING,
                StructType::new_unchecked([StructField::nullable(
                    "x",
                    DataType::VOID,
                )]),
                true,
            ),
        ),
        "struct nested in Array or Map must contain at least one non-void field"
    )]
    fn test_void_in_complex_type_rejected(
        #[case] desc: &str,
        #[case] field: StructField,
        #[case] expected_msg: &str,
    ) {
        let schema = StructType::new_unchecked([field]);
        let result = validate_schema_for_write(&schema);
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
            StructType::new_unchecked([
                StructField::nullable("a", DataType::INTEGER),
                StructField::nullable("b", DataType::VOID),
            ]),
        )])
    )]
    #[case(
        "array of struct without void",
        StructType::new_unchecked([StructField::nullable(
            "arr",
            ArrayType::new(
                DataType::Struct(Box::new(StructType::new_unchecked([
                    StructField::nullable("a", DataType::INTEGER),
                    StructField::nullable("b", DataType::STRING),
                ]))),
                true,
            ),
        )])
    )]
    #[case(
        "map of struct without void",
        StructType::new_unchecked([StructField::nullable(
            "m",
            MapType::new(
                DataType::STRING,
                StructType::new_unchecked([
                    StructField::nullable("a", DataType::INTEGER),
                    StructField::nullable("b", DataType::STRING),
                ]),
                true,
            ),
        )])
    )]
    fn test_valid_schema_for_complex_types(#[case] desc: &str, #[case] schema: StructType) {
        validate_schema_for_write(&schema)
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
            StructType::new_unchecked([
                StructField::nullable("a", DataType::INTEGER),
                StructField::nullable("b", DataType::VOID),
            ]),
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
        "at least one non-void column"
    )]
    #[case(
        "all void struct",
        StructType::new_unchecked([
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable(
                "s",
                StructType::new_unchecked([
                    StructField::nullable("x", DataType::VOID),
                    StructField::nullable("y", DataType::VOID),
                ]),
            ),
        ]),
        "contains no non-void fields"
    )]
    #[case(
        "void in array",
        StructType::new_unchecked([StructField::nullable(
            "arr",
            ArrayType::new(DataType::VOID, true),
        )]),
        "array element type"
    )]
    #[case(
        "void in map",
        StructType::new_unchecked([StructField::nullable(
            "m",
            MapType::new(
                DataType::STRING,
                DataType::VOID,
                true,
            ),
        )]),
        "map value type"
    )]
    #[case(
        "nested all void struct",
        StructType::new_unchecked([
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable(
                "outer",
                StructType::new_unchecked([
                    StructField::nullable(
                        "inner",
                        DataType::Struct(Box::new(StructType::new_unchecked([
                            StructField::nullable("x", DataType::VOID),
                        ]))),
                    ),
                ]),
            ),
        ]),
        "contains no non-void fields"
    )]
    #[case(
        "empty struct at top level",
        StructType::new_unchecked(Vec::<StructField>::new()),
        "at least one non-void column"
    )]
    #[case(
        "nested empty struct",
        StructType::new_unchecked([
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable(
                "s",
                StructType::new_unchecked(
                    Vec::<StructField>::new(),
                ),
            ),
        ]),
        "contains no non-void fields"
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

    // ---- strip_void_from_schema tests ----

    #[rstest::rstest]
    #[case(
        "schema with no void is noop",
        StructType::new_unchecked([
            StructField::nullable("a", DataType::INTEGER),
            StructField::nullable("b", DataType::STRING),
        ]),
        StructType::new_unchecked([
            StructField::nullable("a", DataType::INTEGER),
            StructField::nullable("b", DataType::STRING),
        ])
    )]
    #[case(
        "top-level void is dropped",
        StructType::new_unchecked([
            StructField::nullable("a", DataType::INTEGER),
            StructField::nullable("v", DataType::VOID),
            StructField::nullable("b", DataType::STRING),
        ]),
        StructType::new_unchecked([
            StructField::nullable("a", DataType::INTEGER),
            StructField::nullable("b", DataType::STRING),
        ])
    )]
    #[case(
        "nested struct with mixed void",
        StructType::new_unchecked([StructField::nullable(
            "s",
            StructType::new_unchecked([
                StructField::nullable("a", DataType::INTEGER),
                StructField::nullable("b", DataType::VOID),
                StructField::nullable("c", DataType::STRING),
            ]),
        )]),
        StructType::new_unchecked([StructField::nullable(
            "s",
            StructType::new_unchecked([
                StructField::nullable("a", DataType::INTEGER),
                StructField::nullable("c", DataType::STRING),
            ]),
        )])
    )]
    #[case(
        "deeply nested void",
        StructType::new_unchecked([StructField::nullable(
            "outer",
            StructType::new_unchecked([StructField::nullable(
                "inner",
                DataType::Struct(Box::new(StructType::new_unchecked([
                    StructField::nullable("a", DataType::INTEGER),
                    StructField::nullable("v", DataType::VOID),
                ]))),
            )]),
        )]),
        StructType::new_unchecked([StructField::nullable(
            "outer",
            StructType::new_unchecked([StructField::nullable(
                "inner",
                DataType::Struct(Box::new(StructType::new_unchecked([StructField::nullable(
                    "a",
                    DataType::INTEGER,
                )]))),
            )]),
        )])
    )]
    fn test_strip_void_from_schema(
        #[case] desc: &str,
        #[case] input: StructType,
        #[case] expected: StructType,
    ) {
        let stripped = strip_void_from_schema(Arc::new(input));
        assert_eq!(*stripped, expected, "{desc}");
    }

    // A container that has Void as its only "interior" type collapses when the void
    // primitive is filtered: ArrayType / MapType cannot be reconstructed without their
    // element / key / value, so the containing field disappears.
    #[rstest::rstest]
    #[case::array_of_void(DataType::Array(Box::new(ArrayType::new(DataType::VOID, true))))]
    #[case::map_with_void_value(DataType::Map(Box::new(MapType::new(
        DataType::STRING,
        DataType::VOID,
        true
    ))))]
    #[case::map_with_void_key(DataType::Map(Box::new(MapType::new(
        DataType::VOID,
        DataType::STRING,
        true
    ))))]
    fn test_strip_drops_container_with_void(#[case] field_type: DataType) {
        let schema = Arc::new(StructType::new_unchecked([
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable("c", field_type),
        ]));
        let stripped = strip_void_from_schema(schema);
        assert!(stripped.field("id").is_some());
        assert!(stripped.field("c").is_none());
    }

    #[test]
    fn test_strip_preserves_metadata() {
        let mut s_field = StructField::nullable(
            "s",
            StructType::new_unchecked([
                StructField::nullable("a", DataType::INTEGER),
                StructField::nullable("b", DataType::VOID),
            ]),
        );
        s_field.metadata.insert(
            ColumnMetadataKey::ColumnMappingPhysicalName.as_ref().into(),
            MetadataValue::String("phys_s".into()),
        );
        let schema = Arc::new(StructType::new_unchecked([s_field]));
        let stripped = strip_void_from_schema(schema);
        assert_eq!(
            stripped
                .field("s")
                .expect("s field present after strip")
                .metadata
                .get(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref()),
            Some(&MetadataValue::String("phys_s".into())),
            "metadata should be preserved after stripping"
        );
    }
}
