//! The [`LiteralExpressionTransform`] is a [`SchemaTransform`] that transforms a [`Schema`] and an
//! ordered list of leaf values (scalars) into an [`Expression`] with a literal value for each leaf.

use std::borrow::Cow;
use std::ops::Deref as _;

use tracing::debug;

use crate::expressions::{Expression, Scalar};
use crate::schema::{
    ArrayType, DataType, MapType, PrimitiveType, SchemaTransform, StructField, StructType,
};

/// [`SchemaTransform`] that will transform a [`Schema`] and an ordered list of leaf values
/// (Scalars) into an Expression with a [`Literal`] expr for each leaf.
#[derive(Debug)]
pub(crate) struct LiteralExpressionTransform<'a, T: Iterator<Item = &'a Scalar>> {
    /// Leaf values to insert in schema order.
    scalars: T,
    /// A stack of built Expressions. After visiting children, we pop them off to
    /// build the parent container, then push the parent back on.
    stack: Vec<Expression>,
    /// Since schema transforms are infallible we keep track of errors here
    error: Result<(), Error>,
}

/// Any error for [`LiteralExpressionTransform`]
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Schema mismatch error
    #[error("Schema error: {0}")]
    Schema(String),

    /// Insufficient number of scalars (too many) to create a single-row expression
    #[error("Excess scalar: {0} given for literal expression transform")]
    ExcessScalars(Scalar),

    /// Insufficient number of scalars (too few) to create a single-row expression
    #[error("Too few scalars given for literal expression transform")]
    InsufficientScalars,

    /// Empty expression stack after performing the transform
    #[error("No Expression was created after performing the transform")]
    EmptyStack,

    /// Unsupported operation
    #[error("Unsupported operation: {0}")]
    Unsupported(String),
}

impl<'a, I: Iterator<Item = &'a Scalar>> LiteralExpressionTransform<'a, I> {
    pub(crate) fn new(scalars: impl IntoIterator<IntoIter = I>) -> Self {
        Self {
            scalars: scalars.into_iter(),
            stack: Vec::new(),
            error: Ok(()),
        }
    }

    /// return the Expression we just built (or propagate Error). the top of `stack` should be our
    /// final Expression
    pub(crate) fn try_into_expr(mut self) -> Result<Expression, Error> {
        self.error?;

        if let Some(s) = self.scalars.next() {
            return Err(Error::ExcessScalars(s.clone()));
        }

        self.stack.pop().ok_or(Error::EmptyStack)
    }

    fn set_error(&mut self, error: Error) {
        // Only set when the error not yet set
        if let Err(ref existing_error) = self.error {
            debug!("Trying to overwrite an existing error: {existing_error:?} with {error:?}");
        } else {
            self.error = Err(error);
        }
    }
}

// All leaf types (primitive, array, map) share the same "shape" of transformation logic
macro_rules! transform_leaf {
    ($self:ident, $type_variant:path, $type:ident) => {{
        // first always check error to terminate early if possible
        $self.error.as_ref().ok()?;

        let Some(scalar) = $self.scalars.next() else {
            $self.set_error(Error::InsufficientScalars);
            return None;
        };

        // NOTE: Grab a reference here so code below can leverage the blanket impl<T> Deref for &T
        let $type_variant(ref scalar_type) = scalar.data_type() else {
            $self.set_error(Error::Schema(format!(
                "Mismatched scalar type while creating Expression: expected {}({:?}), got {:?}",
                stringify!($type_variant),
                $type,
                scalar.data_type()
            )));
            return None;
        };

        // NOTE: &T and &Box<T> both deref to &T
        if scalar_type.deref() != $type {
            $self.set_error(Error::Schema(format!(
                "Mismatched scalar type while creating Expression: expected {:?}, got {:?}",
                $type, scalar_type
            )));
            return None;
        }

        $self.stack.push(Expression::Literal(scalar.clone()));
        None
    }};
}

impl<'a, T: Iterator<Item = &'a Scalar>> SchemaTransform<'a> for LiteralExpressionTransform<'a, T> {
    fn transform_primitive(
        &mut self,
        prim_type: &'a PrimitiveType,
    ) -> Option<Cow<'a, PrimitiveType>> {
        transform_leaf!(self, DataType::Primitive, prim_type)
    }

    fn transform_struct(&mut self, struct_type: &'a StructType) -> Option<Cow<'a, StructType>> {
        // first always check error to terminate early if possible
        self.error.as_ref().ok()?;

        // Only consume newly-added entries (if any). There could be fewer than expected if
        // the recursion encountered an error.
        let mark = self.stack.len();
        self.recurse_into_struct(struct_type)?;
        let field_exprs = self.stack.split_off(mark);

        let fields = struct_type.fields();
        if field_exprs.len() != fields.len() {
            self.set_error(Error::InsufficientScalars);
            return None;
        }

        let mut found_non_nullable_null = false;
        let mut all_null = true;
        for (field, expr) in fields.zip(&field_exprs) {
            if !matches!(expr, Expression::Literal(Scalar::Null(_))) {
                all_null = false;
            } else if !field.is_nullable() {
                found_non_nullable_null = true;
            }
        }

        // If all children are NULL and at least one is ostensibly non-nullable, we interpret
        // the struct itself as being NULL (if all aren't null then it's an error)
        let struct_expr = if found_non_nullable_null {
            if !all_null {
                // we found a non_nullable NULL, but other siblings are non-null: error
                self.set_error(Error::Schema(
                    "NULL value for non-nullable struct field with non-NULL siblings".to_string(),
                ));
                return None;
            }
            Expression::null_literal(struct_type.clone().into())
        } else {
            Expression::struct_from(field_exprs)
        };

        self.stack.push(struct_expr);
        None
    }

    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        // first always check error to terminate early if possible
        self.error.as_ref().ok()?;

        self.recurse_into_struct_field(field);
        Some(Cow::Borrowed(field))
    }

    // arrays treated as leaves
    fn transform_array(&mut self, array_type: &'a ArrayType) -> Option<Cow<'a, ArrayType>> {
        transform_leaf!(self, DataType::Array, array_type)
    }

    // maps treated as leaves
    fn transform_map(&mut self, map_type: &'a MapType) -> Option<Cow<'a, MapType>> {
        transform_leaf!(self, DataType::Map, map_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use crate::expressions::{ArrayData, MapData};
    use crate::schema::SchemaRef;
    use crate::schema::StructType;
    use crate::DataType as DeltaDataTypes;

    use paste::paste;

    use Expression as Expr;

    // helper to take values/schema to pass to `create_one` and assert the result = expected
    fn assert_single_row_transform(
        values: &[Scalar],
        schema: SchemaRef,
        expected: Result<Expr, ()>,
    ) {
        let mut schema_transform = LiteralExpressionTransform::new(values);
        let datatype = schema.into();
        let _transformed = schema_transform.transform(&datatype);
        match expected {
            Ok(expected_expr) => {
                let actual_expr = schema_transform.try_into_expr().unwrap();
                // TODO: we can't compare NULLs so we convert with .to_string to workaround
                // see: https://github.com/delta-io/delta-kernel-rs/pull/677
                assert_eq!(expected_expr.to_string(), actual_expr.to_string());
            }
            Err(()) => {
                assert!(schema_transform.try_into_expr().is_err());
            }
        }
    }

    #[test]
    fn test_create_one_top_level_null() {
        let values = &[Scalar::Null(DeltaDataTypes::INTEGER)];

        let schema = Arc::new(StructType::new_unchecked([StructField::not_null(
            "col_1",
            DeltaDataTypes::INTEGER,
        )]));
        let expected = Expr::null_literal(schema.clone().into());
        assert_single_row_transform(values, schema, Ok(expected));

        let schema = Arc::new(StructType::new_unchecked([StructField::nullable(
            "col_1",
            DeltaDataTypes::INTEGER,
        )]));
        let expected = Expr::struct_from(vec![Expr::null_literal(DeltaDataTypes::INTEGER)]);
        assert_single_row_transform(values, schema, Ok(expected));
    }

    #[test]
    fn test_create_one_missing_values() {
        let values = &[1.into()];
        let schema = Arc::new(StructType::new_unchecked([
            StructField::nullable("col_1", DeltaDataTypes::INTEGER),
            StructField::nullable("col_2", DeltaDataTypes::INTEGER),
        ]));
        assert_single_row_transform(values, schema, Err(()));
    }

    #[test]
    fn test_create_one_extra_values() {
        let values = &[1.into(), 2.into(), 3.into()];
        let schema = Arc::new(StructType::new_unchecked([
            StructField::nullable("col_1", DeltaDataTypes::INTEGER),
            StructField::nullable("col_2", DeltaDataTypes::INTEGER),
        ]));
        assert_single_row_transform(values, schema, Err(()));
    }

    #[test]
    fn test_create_one_incorrect_schema() {
        let values = &["a".into()];
        let schema = Arc::new(StructType::new_unchecked([StructField::nullable(
            "col_1",
            DeltaDataTypes::INTEGER,
        )]));
        assert_single_row_transform(values, schema, Err(()));
    }

    // useful test to make sure that we correctly process the stack
    #[test]
    fn test_many_structs() {
        let values: &[Scalar] = &[1.into(), 2.into(), 3.into(), 4.into()];
        let schema = Arc::new(StructType::new_unchecked([
            StructField::nullable(
                "x",
                DeltaDataTypes::struct_type_unchecked([
                    StructField::not_null("a", DeltaDataTypes::INTEGER),
                    StructField::nullable("b", DeltaDataTypes::INTEGER),
                ]),
            ),
            StructField::nullable(
                "y",
                DeltaDataTypes::struct_type_unchecked([
                    StructField::not_null("c", DeltaDataTypes::INTEGER),
                    StructField::nullable("d", DeltaDataTypes::INTEGER),
                ]),
            ),
        ]));
        let expected = Expr::struct_from(vec![
            Expr::struct_from(vec![Expr::literal(1), Expr::literal(2)]),
            Expr::struct_from(vec![Expr::literal(3), Expr::literal(4)]),
        ]);
        assert_single_row_transform(values, schema, Ok(expected));
    }

    #[test]
    fn test_map_and_array() {
        let map_type = MapType::new(DeltaDataTypes::STRING, DeltaDataTypes::STRING, false);
        let map_data = MapData::try_new(map_type.clone(), vec![("k1", "v1")]).unwrap();
        let array_type = ArrayType::new(DeltaDataTypes::INTEGER, false);
        let array_data = ArrayData::try_new(array_type.clone(), vec![1, 2]).unwrap();
        let values: &[Scalar] = &[
            Scalar::Map(map_data.clone()),
            Scalar::Array(array_data.clone()),
        ];
        let schema = Arc::new(StructType::new_unchecked([
            StructField::nullable("map", DeltaDataTypes::Map(Box::new(map_type))),
            StructField::nullable("array", DeltaDataTypes::Array(Box::new(array_type))),
        ]));
        let expected = Expr::struct_from(vec![
            Expr::literal(Scalar::Map(map_data)),
            Expr::literal(Scalar::Array(array_data)),
        ]);
        assert_single_row_transform(values, schema, Ok(expected));
    }

    #[derive(Clone, Copy)]
    struct TestSchema {
        x_nullable: bool,
        a_nullable: bool,
        b_nullable: bool,
    }

    enum Expected {
        Noop,
        NullStruct,
        Null,
        Error, // TODO: we could check the actual error
    }

    fn run_test(test_schema: TestSchema, values: (Option<i32>, Option<i32>), expected: Expected) {
        let (a_val, b_val) = values;
        let a = match a_val {
            Some(v) => Scalar::Integer(v),
            None => Scalar::Null(DeltaDataTypes::INTEGER),
        };
        let b = match b_val {
            Some(v) => Scalar::Integer(v),
            None => Scalar::Null(DeltaDataTypes::INTEGER),
        };
        let values: &[Scalar] = &[a, b];

        let field_a = StructField::new("a", DeltaDataTypes::INTEGER, test_schema.a_nullable);
        let field_b = StructField::new("b", DeltaDataTypes::INTEGER, test_schema.b_nullable);
        let field_x = StructField::new(
            "x",
            StructType::new_unchecked([field_a.clone(), field_b.clone()]),
            test_schema.x_nullable,
        );
        let schema = Arc::new(StructType::new_unchecked([field_x.clone()]));

        let expected_result = match expected {
            Expected::Noop => {
                let nested_struct = Expr::struct_from(vec![
                    Expr::literal(values[0].clone()),
                    Expr::literal(values[1].clone()),
                ]);
                Ok(Expr::struct_from([nested_struct]))
            }
            Expected::Null => Ok(Expr::null_literal(schema.clone().into())),
            Expected::NullStruct => {
                let nested_null = Expr::null_literal(field_x.data_type().clone());
                Ok(Expr::struct_from([nested_null]))
            }
            Expected::Error => Err(()),
        };

        assert_single_row_transform(values, schema, expected_result);
    }

    // helper to convert nullable/not_null to bool
    macro_rules! bool_from_nullable {
        (nullable) => {
            true
        };
        (not_null) => {
            false
        };
    }

    // helper to convert a/b/N to Some/Some/None (1 and 2 just arbitrary non-null ints)
    macro_rules! parse_value {
        (a) => {
            Some(1)
        };
        (b) => {
            Some(2)
        };
        (N) => {
            None
        };
    }

    macro_rules! test_nullability_combinations {
    (
        name = $name:ident,
        schema = { x: $x:ident, a: $a:ident, b: $b:ident },
        tests = {
            ($ta1:tt, $tb1:tt) -> $expected1:ident,
            ($ta2:tt, $tb2:tt) -> $expected2:ident,
            ($ta3:tt, $tb3:tt) -> $expected3:ident,
            ($ta4:tt, $tb4:tt) -> $expected4:ident $(,)?
        }
    ) => {
        paste! {
            #[test]
            fn [<$name _ $ta1:lower _ $tb1:lower>]() {
                let schema = TestSchema {
                    x_nullable: bool_from_nullable!($x),
                    a_nullable: bool_from_nullable!($a),
                    b_nullable: bool_from_nullable!($b),
                };
                run_test(schema, (parse_value!($ta1), parse_value!($tb1)), Expected::$expected1);
            }
            #[test]
            fn [<$name _ $ta2:lower _ $tb2:lower>]() {
                let schema = TestSchema {
                    x_nullable: bool_from_nullable!($x),
                    a_nullable: bool_from_nullable!($a),
                    b_nullable: bool_from_nullable!($b),
                };
                run_test(schema, (parse_value!($ta2), parse_value!($tb2)), Expected::$expected2);
            }
            #[test]
            fn [<$name _ $ta3:lower _ $tb3:lower>]() {
                let schema = TestSchema {
                    x_nullable: bool_from_nullable!($x),
                    a_nullable: bool_from_nullable!($a),
                    b_nullable: bool_from_nullable!($b),
                };
                run_test(schema, (parse_value!($ta3), parse_value!($tb3)), Expected::$expected3);
            }
            #[test]
            fn [<$name _ $ta4:lower _ $tb4:lower>]() {
                let schema = TestSchema {
                    x_nullable: bool_from_nullable!($x),
                    a_nullable: bool_from_nullable!($a),
                    b_nullable: bool_from_nullable!($b),
                };
                run_test(schema, (parse_value!($ta4), parse_value!($tb4)), Expected::$expected4);
            }
        }
    }
    }

    // Group 1: nullable { nullable, nullable }
    //  1. (a, b) -> x (a, b)
    //  2. (N, b) -> x (N, b)
    //  3. (a, N) -> x (a, N)
    //  4. (N, N) -> x (N, N)
    test_nullability_combinations! {
        name = test_all_nullable,
        schema = { x: nullable, a: nullable, b: nullable },
        tests = {
            (a, b) -> Noop,
            (N, b) -> Noop,
            (a, N) -> Noop,
            (N, N) -> Noop,
        }
    }

    // Group 2: nullable { nullable, not_null }
    //  1. (a, b) -> x (a, b)
    //  2. (N, b) -> x (N, b)
    //  3. (a, N) -> Err
    //  4. (N, N) -> x NULL
    test_nullability_combinations! {
        name = test_nullable_nullable_not_null,
        schema = { x: nullable, a: nullable, b: not_null },
        tests = {
            (a, b) -> Noop,
            (N, b) -> Noop,
            (a, N) -> Error,
            (N, N) -> NullStruct,
        }
    }

    // Group 3: nullable { not_null, not_null }
    //  1. (a, b) -> x (a, b)
    //  2. (N, b) -> Err
    //  3. (a, N) -> Err
    //  4. (N, N) -> x NULL
    test_nullability_combinations! {
        name = test_nullable_not_null_not_null,
        schema = { x: nullable, a: not_null, b: not_null },
        tests = {
            (a, b) -> Noop,
            (N, b) -> Error,
            (a, N) -> Error,
            (N, N) -> NullStruct,
        }
    }

    // Group 4: not_null { nullable, nullable }
    //  1. (a, b) -> x (a, b)
    //  2. (N, b) -> x (N, b)
    //  3. (a, N) -> x (a, N)
    //  4. (N, N) -> x (N, N)
    test_nullability_combinations! {
        name = test_not_null_nullable_nullable,
        schema = { x: not_null, a: nullable, b: nullable },
        tests = {
            (a, b) -> Noop,
            (N, b) -> Noop,
            (a, N) -> Noop,
            (N, N) -> Noop,
        }
    }

    // Group 5: not_null { nullable, not_null }
    //  1. (a, b) -> x (a, b)
    //  2. (N, b) -> x (N, b)
    //  3. (a, N) -> Err
    //  4. (N, N) -> NULL
    test_nullability_combinations! {
        name = test_not_null_nullable_not_null,
        schema = { x: not_null, a: nullable, b: not_null },
        tests = {
            (a, b) -> Noop,
            (N, b) -> Noop,
            (a, N) -> Error,
            (N, N) -> Null,
        }
    }

    // Group 6: not_null { not_null, not_null }
    //  1. (a, b) -> x (a, b)
    //  2. (N, b) -> Err
    //  3. (a, N) -> Err
    //  4. (N, N) -> NULL
    test_nullability_combinations! {
        name = test_all_not_null,
        schema = { x: not_null, a: not_null, b: not_null },
        tests = {
            (a, b) -> Noop,
            (N, b) -> Error,
            (a, N) -> Error,
            (N, N) -> Null,
        }
    }
}
