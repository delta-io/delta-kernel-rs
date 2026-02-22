//! Validation for void type usage in schemas.
//!
//! The Delta protocol notes that void columns may exist in tables but behavior is undefined.
//! Spark rejects void nested inside Array or Map types. This module replicates that validation.

use std::borrow::Cow;

use super::{DataType, Schema, SchemaTransform, StructField};
use crate::{DeltaResult, Error};

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
    fn test_no_void_ok() {
        let schema = StructType::new_unchecked([
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable("name", DataType::STRING),
        ]);
        validate_no_void_in_complex_types(&schema).expect("Schema without void should be fine");
    }
}
