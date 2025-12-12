//! A post-order schema visitor for traversing schema types.

use crate::schema::{ArrayType, DataType, MapType, PrimitiveType, StructField, StructType};
use crate::DeltaResult;

/// A post-order schema visitor.
///
/// This visitor traverses schema types, calling methods as each type is encountered.
/// For struct types, fields are visited first, then the struct itself. For primitives,
/// arrays, and maps, the respective methods are called directly (arrays and maps are
/// treated as leaves - their element/key/value types are not automatically visited).
pub(crate) trait SchemaVisitor {
    /// Return type of this visitor.
    type T;

    /// Called after a struct field's data type is visited.
    fn field(&mut self, field: &StructField, value: Self::T) -> DeltaResult<Self::T>;

    /// Called after all of a struct's fields have been visited.
    fn r#struct(&mut self, r#struct: &StructType, results: Vec<Self::T>) -> DeltaResult<Self::T>;

    /// Called when encountering an array type (treated as a leaf - element type is not visited).
    fn array(&mut self, array: &ArrayType) -> DeltaResult<Self::T>;

    /// Called when encountering a map type (treated as a leaf - key/value types are not visited).
    fn map(&mut self, map: &MapType) -> DeltaResult<Self::T>;

    /// Called when encountering a primitive type.
    fn primitive(&mut self, p: &PrimitiveType) -> DeltaResult<Self::T>;

    /// Called when encountering a variant type.
    fn variant(&mut self, r#struct: &StructType) -> DeltaResult<Self::T>;
}

/// Visit a data type in post order.
///
/// For structs, fields are visited first (recursively), then the struct itself.
/// Arrays and maps are treated as leaves - their element/key/value types are not visited.
#[allow(dead_code)] // Reserved for future use
pub(crate) fn visit_type<V: SchemaVisitor>(
    r#type: &DataType,
    visitor: &mut V,
) -> DeltaResult<V::T> {
    match r#type {
        DataType::Primitive(p) => visitor.primitive(p),
        DataType::Array(array) => visitor.array(array),
        DataType::Map(map) => visitor.map(map),
        DataType::Struct(s) => visit_struct(s, visitor),
        DataType::Variant(v) => visitor.variant(v),
    }
}

/// Visit a struct type in post order.
///
/// Each field's data type is visited first, then the field callback is invoked,
/// and finally the struct callback is invoked with all field results.
pub(crate) fn visit_struct<V: SchemaVisitor>(s: &StructType, visitor: &mut V) -> DeltaResult<V::T> {
    let mut results = Vec::with_capacity(s.fields().len());
    for field in s.fields() {
        let result = visit_type(&field.data_type, visitor)?;
        let result = visitor.field(field, result)?;
        results.push(result);
    }

    visitor.r#struct(s, results)
}
