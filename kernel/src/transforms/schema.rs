use std::borrow::Cow;

use crate::schema::{ArrayType, DataType, MapType, PrimitiveType, StructField, StructType};
use crate::transforms::{map_owned_children_or_else, CowExt as _};

/// Generic framework for describing recursive bottom-up schema transforms. Transformations return
/// `Option<Cow>` with the following semantics:
/// * `Some(Cow::Owned)` -- The schema element was transformed and should propagate to its parent.
/// * `Some(Cow::Borrowed)` -- The schema element was not transformed.
/// * `None` -- The schema element was filtered out and the parent should no longer reference it.
///
/// The transform can start from whatever schema element is available
/// (e.g. [`Self::transform_struct`] to start with [`StructType`]), or it can start from the generic
/// [`Self::transform`].
///
/// The provided `transform_xxx` methods all default to no-op, and implementations should
/// selectively override specific `transform_xxx` methods as needed for the task at hand.
///
/// The provided `recurse_into_xxx` methods encapsulate the boilerplate work of recursing into the
/// child schema elements of each schema element. Implementations can call these as needed but will
/// generally not need to override them.
pub trait SchemaTransform<'a> {
    /// Called for each primitive encountered during the schema traversal.
    fn transform_primitive(&mut self, ptype: &'a PrimitiveType) -> Option<Cow<'a, PrimitiveType>> {
        Some(Cow::Borrowed(ptype))
    }

    /// Called for each struct encountered during the schema traversal. Implementations can call
    /// [`Self::recurse_into_struct`] if they wish to recursively transform the struct's fields.
    fn transform_struct(&mut self, stype: &'a StructType) -> Option<Cow<'a, StructType>> {
        self.recurse_into_struct(stype)
    }

    /// Called for each struct field encountered during the schema traversal. Implementations can
    /// call [`Self::recurse_into_struct_field`] if they wish to recursively transform the field's
    /// data type.
    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        self.recurse_into_struct_field(field)
    }

    /// Called for each array encountered during the schema traversal. Implementations can call
    /// [`Self::recurse_into_array`] if they wish to recursively transform the array's element type.
    fn transform_array(&mut self, atype: &'a ArrayType) -> Option<Cow<'a, ArrayType>> {
        self.recurse_into_array(atype)
    }

    /// Called for each array element encountered during the schema traversal. Implementations can
    /// call [`Self::transform`] if they wish to recursively transform the array element type.
    fn transform_array_element(&mut self, etype: &'a DataType) -> Option<Cow<'a, DataType>> {
        self.transform(etype)
    }

    /// Called for each map encountered during the schema traversal. Implementations can call
    /// [`Self::recurse_into_map`] if they wish to recursively transform the map's key and/or value
    /// types.
    fn transform_map(&mut self, mtype: &'a MapType) -> Option<Cow<'a, MapType>> {
        self.recurse_into_map(mtype)
    }

    /// Called for each map key encountered during the schema traversal. Implementations can call
    /// [`Self::transform`] if they wish to recursively transform the map key type.
    fn transform_map_key(&mut self, etype: &'a DataType) -> Option<Cow<'a, DataType>> {
        self.transform(etype)
    }

    /// Called for each map value encountered during the schema traversal. Implementations can call
    /// [`Self::transform`] if they wish to recursively transform the map value type.
    fn transform_map_value(&mut self, etype: &'a DataType) -> Option<Cow<'a, DataType>> {
        self.transform(etype)
    }

    /// Called for each variant value encountered. By default, recurses into the fields of the
    /// variant struct type.
    fn transform_variant(&mut self, stype: &'a StructType) -> Option<Cow<'a, StructType>> {
        self.recurse_into_struct(stype)
    }

    /// General entry point for a recursive traversal over any data type. Also invoked internally to
    /// dispatch on nested data types encountered during the traversal.
    fn transform(&mut self, data_type: &'a DataType) -> Option<Cow<'a, DataType>> {
        use DataType::*;
        let result = match data_type {
            Primitive(ptype) => self
                .transform_primitive(ptype)?
                .map_owned_or_else(data_type, DataType::from),
            Array(atype) => self
                .transform_array(atype)?
                .map_owned_or_else(data_type, DataType::from),
            Struct(stype) => self
                .transform_struct(stype)?
                .map_owned_or_else(data_type, DataType::from),
            Map(mtype) => self
                .transform_map(mtype)?
                .map_owned_or_else(data_type, DataType::from),
            Variant(stype) => self
                .transform_variant(stype)?
                .map_owned_or_else(data_type, |s| DataType::Variant(Box::new(s))),
        };
        Some(result)
    }

    /// Recursively transforms a struct field's data type. If the data type changes, update the
    /// field to reference it. Otherwise, no-op.
    fn recurse_into_struct_field(
        &mut self,
        field: &'a StructField,
    ) -> Option<Cow<'a, StructField>> {
        let result = self.transform(&field.data_type)?;
        let f = |new_data_type| StructField {
            name: field.name.clone(),
            data_type: new_data_type,
            nullable: field.nullable,
            metadata: field.metadata.clone(),
        };
        Some(result.map_owned_or_else(field, f))
    }

    /// Recursively transforms a struct's fields. If one or more fields were changed or removed,
    /// update the struct to reference all surviving fields. Otherwise, no-op.
    fn recurse_into_struct(&mut self, stype: &'a StructType) -> Option<Cow<'a, StructType>> {
        let transformed_children = stype.fields().map(|f| self.transform_struct_field(f));
        map_owned_children_or_else(stype, transformed_children, StructType::new_unchecked)
    }

    /// Recursively transforms an array's element type. If the element type changes, update the
    /// array to reference it. Otherwise, no-op.
    fn recurse_into_array(&mut self, atype: &'a ArrayType) -> Option<Cow<'a, ArrayType>> {
        let result = self.transform_array_element(&atype.element_type)?;
        let f = |element_type| ArrayType {
            type_name: atype.type_name.clone(),
            element_type,
            contains_null: atype.contains_null,
        };
        Some(result.map_owned_or_else(atype, f))
    }

    /// Recursively transforms a map's key and value types. If either one changes, update the map to
    /// reference them. If either one is removed, remove the map as well. Otherwise, no-op.
    fn recurse_into_map(&mut self, mtype: &'a MapType) -> Option<Cow<'a, MapType>> {
        let key_type = self.transform_map_key(&mtype.key_type)?;
        let value_type = self.transform_map_value(&mtype.value_type)?;
        let f = |(key_type, value_type)| MapType {
            type_name: mtype.type_name.clone(),
            key_type,
            value_type,
            value_contains_null: mtype.value_contains_null,
        };
        Some((key_type, value_type).map_owned_or_else(mtype, f))
    }
}

/// A schema "transform" that doesn't actually change the schema at all. Instead, it measures the
/// maximum depth of a schema, with a depth limit to prevent stack overflow. Useful for verifying
/// that a schema has reasonable depth before attempting to work with it.
pub struct SchemaDepthChecker {
    depth_limit: usize,
    max_depth_seen: usize,
    current_depth: usize,
    call_count: usize,
}
impl SchemaDepthChecker {
    /// Depth-checks the given data type against a given depth limit. The return value is the
    /// largest depth seen, which is capped at one more than the depth limit (indicating the
    /// recursion was terminated).
    pub fn check(data_type: &DataType, depth_limit: usize) -> usize {
        Self::check_with_call_count(data_type, depth_limit).0
    }

    // Exposed for testing
    fn check_with_call_count(data_type: &DataType, depth_limit: usize) -> (usize, usize) {
        let mut checker = Self {
            depth_limit,
            max_depth_seen: 0,
            current_depth: 0,
            call_count: 0,
        };
        checker.transform(data_type);
        (checker.max_depth_seen, checker.call_count)
    }

    // Triggers the requested recursion only doing so would not exceed the depth limit.
    fn depth_limited<'a, T: Clone + std::fmt::Debug>(
        &mut self,
        recurse: impl FnOnce(&mut Self, &'a T) -> Option<Cow<'a, T>>,
        arg: &'a T,
    ) -> Option<Cow<'a, T>> {
        self.call_count += 1;
        if self.max_depth_seen < self.current_depth {
            self.max_depth_seen = self.current_depth;
            if self.depth_limit < self.current_depth {
                tracing::warn!("Max schema depth {} exceeded by {arg:?}", self.depth_limit);
            }
        }
        if self.max_depth_seen <= self.depth_limit {
            self.current_depth += 1;
            let _ = recurse(self, arg);
            self.current_depth -= 1;
        }
        None
    }
}
impl<'a> SchemaTransform<'a> for SchemaDepthChecker {
    fn transform_struct(&mut self, stype: &'a StructType) -> Option<Cow<'a, StructType>> {
        self.depth_limited(Self::recurse_into_struct, stype)
    }
    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        self.depth_limited(Self::recurse_into_struct_field, field)
    }
    fn transform_array(&mut self, atype: &'a ArrayType) -> Option<Cow<'a, ArrayType>> {
        self.depth_limited(Self::recurse_into_array, atype)
    }
    fn transform_map(&mut self, mtype: &'a MapType) -> Option<Cow<'a, MapType>> {
        self.depth_limited(Self::recurse_into_map, mtype)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{DataType, StructField};

    #[test]
    fn test_depth_checker() {
        let schema = DataType::try_struct_type([
            StructField::nullable(
                "a",
                ArrayType::new(
                    DataType::try_struct_type([
                        StructField::nullable("w", DataType::LONG),
                        StructField::nullable("x", ArrayType::new(DataType::LONG, true)),
                        StructField::nullable(
                            "y",
                            MapType::new(DataType::LONG, DataType::STRING, true),
                        ),
                        StructField::nullable(
                            "z",
                            DataType::try_struct_type([
                                StructField::nullable("n", DataType::LONG),
                                StructField::nullable("m", DataType::STRING),
                            ])
                            .unwrap(),
                        ),
                    ])
                    .unwrap(),
                    true,
                ),
            ),
            StructField::nullable(
                "b",
                DataType::try_struct_type([
                    StructField::nullable("o", ArrayType::new(DataType::LONG, true)),
                    StructField::nullable(
                        "p",
                        MapType::new(DataType::LONG, DataType::STRING, true),
                    ),
                    StructField::nullable(
                        "q",
                        DataType::try_struct_type([
                            StructField::nullable(
                                "s",
                                DataType::try_struct_type([
                                    StructField::nullable("u", DataType::LONG),
                                    StructField::nullable("v", DataType::LONG),
                                ])
                                .unwrap(),
                            ),
                            StructField::nullable("t", DataType::LONG),
                        ])
                        .unwrap(),
                    ),
                    StructField::nullable("r", DataType::LONG),
                ])
                .unwrap(),
            ),
            StructField::nullable(
                "c",
                MapType::new(
                    DataType::LONG,
                    DataType::try_struct_type([
                        StructField::nullable("f", DataType::LONG),
                        StructField::nullable("g", DataType::STRING),
                    ])
                    .unwrap(),
                    true,
                ),
            ),
        ])
        .unwrap();

        // Similar to SchemaDepthChecker::check, but also returns call count
        let check_with_call_count =
            |depth_limit| SchemaDepthChecker::check_with_call_count(&schema, depth_limit);

        // Hit depth limit at "a" but still have to look at "b" "c" "d"
        assert_eq!(check_with_call_count(1), (2, 5));
        assert_eq!(check_with_call_count(2), (3, 6));

        // Hit depth limit at "w" but still have to look at "x" "y" "z"
        assert_eq!(check_with_call_count(3), (4, 10));
        assert_eq!(check_with_call_count(4), (5, 11));

        // Depth limit hit at "n" but still have to look at "m"
        assert_eq!(check_with_call_count(5), (6, 15));

        // Depth limit not hit until "u"
        assert_eq!(check_with_call_count(6), (7, 28));

        // Depth limit not hit (full traversal required)
        assert_eq!(check_with_call_count(7), (7, 32));
        assert_eq!(check_with_call_count(8), (7, 32));
    }
}
