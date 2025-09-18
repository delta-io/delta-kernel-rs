use crate::schema::ArrayType;
use delta_kernel::schema::{DataType, MapType, PrimitiveType, StructField, StructType};
use delta_kernel::DeltaResult;

/// A post order schema visitor.
///
/// For order of methods called, please refer to [`visit_schema`].
pub(crate) trait SchemaVisitor {
    /// Return type of this visitor.
    type T;

    /// Called after struct's field type visited.
    fn field(&mut self, field: &StructField, value: Self::T) -> DeltaResult<Self::T>;
    /// Called after struct's fields visited.
    fn r#struct(&mut self, r#struct: &StructType, results: Vec<Self::T>) -> DeltaResult<Self::T>;
    /// Called after list fields visited.
    fn list(&mut self, list: &ArrayType, value: Self::T) -> DeltaResult<Self::T>;
    /// Called after map's key and value fields visited.
    fn map(&mut self, map: &MapType, key_value: Self::T, value: Self::T) -> DeltaResult<Self::T>;
    /// Called when see a primitive type.
    fn primitive(&mut self, p: &PrimitiveType) -> DeltaResult<Self::T>;
    /// Called when see a primitive type.
    fn variant(&mut self, r#struct: &StructType) -> DeltaResult<Self::T>;
}

/// Visiting a type in post order.
#[allow(dead_code)] // Reserved for future use
pub(crate) fn visit_type<V: SchemaVisitor>(
    r#type: &DataType,
    visitor: &mut V,
) -> DeltaResult<V::T> {
    match r#type {
        DataType::Primitive(p) => visitor.primitive(p),
        DataType::Array(list) => {
            let value = visit_type(&list.element_type, visitor)?;
            visitor.list(list, value)
        }
        DataType::Map(map) => {
            let key_result = visit_type(&map.key_type, visitor)?;
            let value_result = visit_type(&map.value_type, visitor)?;

            visitor.map(map, key_result, value_result)
        }
        DataType::Struct(s) => visit_struct(s, visitor),
        DataType::Variant(v) => visitor.variant(v),
    }
}

/// Visit struct type in post order.
pub(crate) fn visit_struct<V: SchemaVisitor>(s: &StructType, visitor: &mut V) -> DeltaResult<V::T> {
    let mut results = Vec::with_capacity(s.fields().len());
    for field in s.fields() {
        let result = visit_type(&field.data_type, visitor)?;
        let result = visitor.field(field, result)?;
        results.push(result);
    }

    visitor.r#struct(s, results)
}
