//! Validation and filtering for void type usage in schemas.
//!
//! The Delta protocol notes that void columns may exist in tables but behavior is undefined.
//! Following the Spark connector behavior:
//! - Void fields are recursively dropped from structs at any nesting level during reads.
//! - Void nested inside Array or Map types is rejected with an error.

use std::borrow::Cow;
use std::sync::Arc;

use super::{DataType, Schema, SchemaTransform, StructField, StructType};
use crate::{DeltaResult, Error};

/// Schema transform that recursively removes void fields from structs at any nesting level.
/// Mirrors Spark's `dropNullTypeColumns` behavior.
struct DropVoidFields;

impl<'a> SchemaTransform<'a> for DropVoidFields {
    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        if matches!(
            field.data_type(),
            DataType::Primitive(super::PrimitiveType::Void)
        ) {
            return None; // Remove this field
        }
        self.recurse_into_struct_field(field)
    }
}

/// Recursively removes void fields from a schema at any nesting level.
/// Returns a new schema with all void fields stripped from structs.
pub(crate) fn drop_void_fields(schema: &StructType) -> Arc<StructType> {
    let mut dropper = DropVoidFields;
    match dropper.transform_struct(schema) {
        Some(Cow::Borrowed(_)) => Arc::new(schema.clone()),
        Some(Cow::Owned(s)) => Arc::new(s),
        None => Arc::new(StructType::new_unchecked(std::iter::empty::<StructField>())),
    }
}

/// Schema visitor that detects void type nested inside Array or Map.
#[derive(Debug, Default)]
struct CheckVoidInComplexTypes(Option<&'static str>);

impl<'a> SchemaTransform<'a> for CheckVoidInComplexTypes {
    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        if self.0.is_some() {
            return Some(Cow::Borrowed(field));
        }
        self.recurse_into_struct_field(field)
    }

    fn transform_array_element(&mut self, etype: &'a DataType) -> Option<Cow<'a, DataType>> {
        if *etype == DataType::VOID {
            self.0 = Some("Void type is not allowed as an array element type");
            return Some(Cow::Borrowed(etype));
        }
        self.transform(etype)
    }

    fn transform_map_key(&mut self, etype: &'a DataType) -> Option<Cow<'a, DataType>> {
        if *etype == DataType::VOID {
            self.0 = Some("Void type is not allowed as a map key type");
            return Some(Cow::Borrowed(etype));
        }
        self.transform(etype)
    }

    fn transform_map_value(&mut self, etype: &'a DataType) -> Option<Cow<'a, DataType>> {
        if *etype == DataType::VOID {
            self.0 = Some("Void type is not allowed as a map value type");
            return Some(Cow::Borrowed(etype));
        }
        self.transform(etype)
    }
}

/// Validates that a schema does not contain void type nested inside Array or Map.
/// Top-level void columns are allowed (they are dropped at scan time).
pub(crate) fn validate_no_void_in_complex_types(schema: &Schema) -> DeltaResult<()> {
    let mut checker = CheckVoidInComplexTypes::default();
    let _ = checker.transform_struct(schema);
    if let Some(msg) = checker.0 {
        return Err(Error::schema(msg));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ArrayType, DataType, MapType, StructField, StructType};

    #[test]
    fn test_void_in_array_rejected() {
        let schema = StructType::new_unchecked([StructField::nullable(
            "arr",
            DataType::Array(Box::new(ArrayType::new(DataType::VOID, true))),
        )]);
        let result = validate_no_void_in_complex_types(&schema);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("array element type"));
    }

    #[test]
    fn test_void_in_map_value_rejected() {
        let schema = StructType::new_unchecked([StructField::nullable(
            "m",
            DataType::Map(Box::new(MapType::new(
                DataType::STRING,
                DataType::VOID,
                true,
            ))),
        )]);
        let result = validate_no_void_in_complex_types(&schema);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("map value type"));
    }

    #[test]
    fn test_void_in_map_key_rejected() {
        let schema = StructType::new_unchecked([StructField::nullable(
            "m",
            DataType::Map(Box::new(MapType::new(
                DataType::VOID,
                DataType::STRING,
                true,
            ))),
        )]);
        let result = validate_no_void_in_complex_types(&schema);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("map key type"));
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
    fn test_drop_void_top_level() {
        let schema = StructType::new_unchecked([
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable("void_col", DataType::VOID),
            StructField::nullable("name", DataType::STRING),
        ]);
        let result = drop_void_fields(&schema);
        assert_eq!(result.fields().count(), 2);
        assert!(result.field("id").is_some());
        assert!(result.field("name").is_some());
        assert!(result.field("void_col").is_none());
    }

    #[test]
    fn test_drop_void_nested_in_struct() {
        // struct<b: void, c: int> → struct<c: int>
        let schema = StructType::new_unchecked([StructField::nullable(
            "outer",
            DataType::Struct(Box::new(StructType::new_unchecked([
                StructField::nullable("b", DataType::VOID),
                StructField::nullable("c", DataType::INTEGER),
            ]))),
        )]);
        let result = drop_void_fields(&schema);
        assert_eq!(result.fields().count(), 1);
        let outer = result.field("outer").expect("outer should exist");
        if let DataType::Struct(inner) = outer.data_type() {
            assert!(
                inner.field("b").is_none(),
                "void field 'b' should be dropped"
            );
            assert!(
                inner.field("c").is_some(),
                "non-void field 'c' should remain"
            );
            assert_eq!(inner.fields().count(), 1);
        } else {
            panic!("outer should be a struct");
        }
    }

    #[test]
    fn test_drop_void_deeply_nested() {
        // a: struct<b: struct<c: void, d: int>> → a: struct<b: struct<d: int>>
        let schema = StructType::new_unchecked([StructField::nullable(
            "a",
            DataType::Struct(Box::new(StructType::new_unchecked([
                StructField::nullable(
                    "b",
                    DataType::Struct(Box::new(StructType::new_unchecked([
                        StructField::nullable("c", DataType::VOID),
                        StructField::nullable("d", DataType::INTEGER),
                    ]))),
                ),
            ]))),
        )]);
        let result = drop_void_fields(&schema);
        let a = result.field("a").expect("a should exist");
        if let DataType::Struct(b_struct) = a.data_type() {
            let b = b_struct.field("b").expect("b should exist");
            if let DataType::Struct(inner) = b.data_type() {
                assert!(
                    inner.field("c").is_none(),
                    "void field 'c' should be dropped"
                );
                assert!(inner.field("d").is_some());
            } else {
                panic!("b should be a struct");
            }
        } else {
            panic!("a should be a struct");
        }
    }

    #[test]
    fn test_drop_void_no_void_unchanged() {
        let schema = StructType::new_unchecked([
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable("name", DataType::STRING),
        ]);
        let result = drop_void_fields(&schema);
        assert_eq!(result.fields().count(), 2);
    }

    #[test]
    fn test_no_void_ok() {
        let schema = StructType::new_unchecked([
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable("name", DataType::STRING),
        ]);
        validate_no_void_in_complex_types(&schema).expect("Schema without void should be fine");
    }
}
