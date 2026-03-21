use std::borrow::{Cow, ToOwned};

use crate::schema::{ArrayType, DataType, MapType, PrimitiveType, StructField, StructType};
use crate::transforms::{
    map_owned_children_or_else, map_owned_or_else, map_owned_pair_or_else, transform_output_type,
    Carrier,
};

/// Generic framework for describing recursive bottom-up schema transforms.
///
/// The transform can start from whatever schema element is available
/// (e.g. [`Self::transform_struct`] to start with [`StructType`]), or it can start from the generic
/// [`Self::transform`].
///
/// The provided `transform_xxx` methods all default to no-op (usually by invoking the corresponding
/// recursive helper method), and implementations should selectively override specific
/// `transform_xxx` methods as needed for the task at hand.
///
/// # Recursive helper methods
///
/// The provided `recurse_into_xxx` methods encapsulate the boilerplate work of recursing into the
/// child schema elements of each schema element. Except as specifically noted otherwise, these
/// recursive helpers all behave uniformly, based on the number of children the schema element has:
///
/// * Leaf (no children) - Leaf `transform_xxx` methods simply return their argument unchanged, and
///   no corresponding `recurse_into_xxx` method is provided.
///
/// * Unary (single child) - If the child was filtered out, filter out the parent. If the child
///   changed, build a new parent around it. Otherwise, return the parent unchanged.
///
/// * Binary (two children) - If either child was filtered out, filter out the parent. If at least
///   one child changed, build a new parent around them. Otherwise, return the parent unchanged.
///
/// * Variadic (0+ children) - If no children remain (all filtered out), filter out the
///   parent. Otherwise, if at least one child changed or was filtered out, build a new parent around
///   the children. Otherwise, return the parent unchanged.
///
/// Implementations can call these as needed, but will generally not need to override them.
///
/// # Transform carrier selection
///
/// Implementations choose an output [`Carrier`] instance based on the operation to be
/// performed. That carrier determines the return type of each transform method.
///
/// For example, a simple read-only visitor would use `()` as a carrier, while a validity checker
/// could use `DeltaResult<()>` instead. A mutating transform uses `Cow<_>`, returning `Cow::Owned`
/// for changed/replaced nodes, and a filtering transform uses `Option<Cow<_>>`, where `None`
/// indicates the node should be dropped rather than replaced. `DeltaResult<Cow<_>>` and
/// `Result<Option<Cow<_>>, E>` round out the set as fallible mutating and fitering transforms that
/// short circuit immediately upon `Err`.
pub trait SchemaTransform<'a> {
    /// [`Carrier`] output type for transformed nodes.
    ///
    /// Implementations can use [`crate::transforms::transform_output_type`] to define `Output`
    /// and `Residual` together.
    type Output<T: ToOwned + ?Sized + 'a>: Carrier<'a, T, Residual = Self::Residual>;

    /// Residual type propagated by this transform's output [`Carrier`]. This type is statically
    /// derivable, but still required due to limitations in rust type system expressiveness.
    ///
    /// Implementations can use [`crate::transforms::transform_output_type`] to define `Output`
    /// and `Residual` together. Or, define it manually like this:
    /// ```rust,no_run
    /// # use std::borrow::Cow;
    /// # use delta_kernel::schema::SchemaTransform;
    /// # use delta_kernel::transforms::Carrier;
    /// # struct X;
    /// # impl<'a> SchemaTransform<'a> for X {
    /// #     type Output<T: std::borrow::ToOwned + ?Sized + 'a> = Cow<'a, T>;
    /// type Residual = <Self::Output<()> as Carrier<'a, ()>>::Residual;
    /// # }
    /// ```
    type Residual;

    /// Called for each primitive encountered during the traversal (leaf).
    fn transform_primitive(&mut self, ptype: &'a PrimitiveType) -> Self::Output<PrimitiveType> {
        Carrier::from_inner(Cow::Borrowed(ptype))
    }

    /// Called for each struct encountered during the traversal. The provided implementation just
    /// forwards to [`Self::recurse_into_struct`].
    fn transform_struct(&mut self, stype: &'a StructType) -> Self::Output<StructType> {
        self.recurse_into_struct(stype)
    }

    /// Called for each struct field encountered during the traversal. The provided implementation
    /// forwards to [`Self::recurse_into_struct_field`].
    fn transform_struct_field(&mut self, field: &'a StructField) -> Self::Output<StructField> {
        self.recurse_into_struct_field(field)
    }

    /// Called for each array encountered during the traversal. The provided implementation just
    /// forwards to [`Self::recurse_into_array`].
    fn transform_array(&mut self, atype: &'a ArrayType) -> Self::Output<ArrayType> {
        self.recurse_into_array(atype)
    }

    /// Called for each array element type encountered during the traversal. The provided
    /// implementation forwards to [`Self::transform`].
    fn transform_array_element(&mut self, etype: &'a DataType) -> Self::Output<DataType> {
        self.transform(etype)
    }

    /// Called for each map encountered during the traversal. The provided implementation just
    /// forwards to [`Self::recurse_into_map`].
    fn transform_map(&mut self, mtype: &'a MapType) -> Self::Output<MapType> {
        self.recurse_into_map(mtype)
    }

    /// Called for each map key encountered during the traversal. The provided implementation
    /// forwards to [`Self::transform`].
    fn transform_map_key(&mut self, etype: &'a DataType) -> Self::Output<DataType> {
        self.transform(etype)
    }

    /// Called for each map value encountered during the traversal. The provided implementation
    /// forwards to [`Self::transform`].
    fn transform_map_value(&mut self, etype: &'a DataType) -> Self::Output<DataType> {
        self.transform(etype)
    }

    /// Called for each variant value encountered. The provided implementation just
    /// forwards to [`Self::recurse_into_struct`].
    fn transform_variant(&mut self, stype: &'a StructType) -> Self::Output<StructType> {
        self.recurse_into_struct(stype)
    }

    /// General entry point for a recursive traversal over any data type. Also invoked internally to
    /// dispatch on nested data types encountered during the traversal.
    fn transform(&mut self, data_type: &'a DataType) -> Self::Output<DataType> {
        match data_type {
            DataType::Primitive(ptype) => {
                let child = self.transform_primitive(ptype);
                map_owned_or_else(data_type, child, DataType::from)
            }
            DataType::Array(atype) => {
                let child = self.transform_array(atype);
                map_owned_or_else(data_type, child, DataType::from)
            }
            DataType::Struct(stype) => {
                let child = self.transform_struct(stype);
                map_owned_or_else(data_type, child, DataType::from)
            }
            DataType::Map(mtype) => {
                let child = self.transform_map(mtype);
                map_owned_or_else(data_type, child, DataType::from)
            }
            DataType::Variant(stype) => {
                let child = self.transform_variant(stype);
                map_owned_or_else(data_type, child, |s| DataType::Variant(Box::new(s)))
            }
        }
    }

    /// Recursively transforms a struct field's data type (unary).
    fn recurse_into_struct_field(&mut self, field: &'a StructField) -> Self::Output<StructField> {
        let child = self.transform(&field.data_type);
        map_owned_or_else(field, child, |new_data_type| StructField {
            name: field.name.clone(),
            data_type: new_data_type,
            nullable: field.nullable,
            metadata: field.metadata.clone(),
        })
    }

    /// Recursively transforms a struct's fields (variadic).
    fn recurse_into_struct(&mut self, stype: &'a StructType) -> Self::Output<StructType> {
        let children = stype.fields().map(|f| self.transform_struct_field(f));
        map_owned_children_or_else(stype, children, StructType::new_unchecked)
    }

    /// Recursively transforms an array's element type (unary).
    fn recurse_into_array(&mut self, atype: &'a ArrayType) -> Self::Output<ArrayType> {
        let child = self.transform_array_element(&atype.element_type);
        map_owned_or_else(atype, child, |element_type| ArrayType {
            type_name: atype.type_name.clone(),
            element_type,
            contains_null: atype.contains_null,
        })
    }

    /// Recursively transforms a map's key and value types (binary).
    fn recurse_into_map(&mut self, mtype: &'a MapType) -> Self::Output<MapType> {
        let key_type = self.transform_map_key(&mtype.key_type);
        let value_type = self.transform_map_value(&mtype.value_type);
        let f = |(key_type, value_type)| MapType {
            type_name: mtype.type_name.clone(),
            key_type,
            value_type,
            value_contains_null: mtype.value_contains_null,
        };
        map_owned_pair_or_else(mtype, key_type, value_type, f)
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
        let _ = checker.transform(data_type);
        (checker.max_depth_seen, checker.call_count)
    }

    // Triggers the requested recursion only if doing so would not exceed the depth limit.
    // If the next step would exceed the limit, short-circuit the traversal.
    fn depth_limited<'a, T: ToOwned + std::fmt::Debug + ?Sized>(
        &mut self,
        recurse: impl FnOnce(&mut Self, &'a T) -> Result<(), ()>,
        arg: &'a T,
    ) -> Result<(), ()> {
        self.call_count += 1;
        self.max_depth_seen = self.max_depth_seen.max(self.current_depth);
        if self.current_depth > self.depth_limit {
            tracing::warn!("Max schema depth {} exceeded by {arg:?}", self.depth_limit);
            return Err(());
        }
        self.current_depth += 1;
        let result = recurse(self, arg);
        self.current_depth -= 1;
        result
    }
}
impl<'a> SchemaTransform<'a> for SchemaDepthChecker {
    transform_output_type!(|'a, T| Result<(), ()>);

    fn transform_struct(&mut self, stype: &'a StructType) -> Result<(), ()> {
        self.depth_limited(Self::recurse_into_struct, stype)
    }
    fn transform_struct_field(&mut self, field: &'a StructField) -> Result<(), ()> {
        self.depth_limited(Self::recurse_into_struct_field, field)
    }
    fn transform_array(&mut self, atype: &'a ArrayType) -> Result<(), ()> {
        self.depth_limited(Self::recurse_into_array, atype)
    }
    fn transform_map(&mut self, mtype: &'a MapType) -> Result<(), ()> {
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

        // Short-circuit when the first over-limit node is encountered.
        assert_eq!(check_with_call_count(1), (2, 3));
        assert_eq!(check_with_call_count(2), (3, 4));

        assert_eq!(check_with_call_count(3), (4, 5));
        assert_eq!(check_with_call_count(4), (5, 7));

        assert_eq!(check_with_call_count(5), (6, 12));

        assert_eq!(check_with_call_count(6), (7, 24));

        // Depth limit not hit (full traversal required)
        assert_eq!(check_with_call_count(7), (7, 32));
        assert_eq!(check_with_call_count(8), (7, 32));
    }
}
