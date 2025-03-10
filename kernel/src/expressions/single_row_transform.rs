use std::borrow::Cow;
use tracing::debug;

use crate::expressions::{Expression, Scalar};
use crate::schema::{
    ArrayType, DataType, MapType, PrimitiveType, SchemaTransform, StructField, StructType,
};

/// [`SchemaTransform`] that will transform a [`Schema`] and an ordered list of leaf values (as
/// Scalar slice) into an Expression with a literal value for each leaf.
#[derive(Debug)]
pub(crate) struct SingleRowTransform<'a, T: Iterator<Item = &'a Scalar>> {
    /// Leaf-node values to insert in schema order.
    scalars: T,
    /// A stack of built Expressions. After visiting children, we pop them off to
    /// build the parent container, then push the parent back on.
    stack: Vec<Expression>,
    /// If an error occurs, it will be stored here.
    error: Option<Error>,
}

/// Any error for the single-row array transform.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Schema mismatch error
    #[error("Schema error: {0}")]
    Schema(String),

    /// Insufficient number of scalars (too many) to create a single-row array
    #[error("Too many scalars given create a single-row array")]
    ExcessScalars,

    /// Insufficient number of scalars (too few) to create a single-row array
    #[error("Too few scalars given create a single-row array")]
    InsufficientScalars,

    /// Empty array after performing the transform
    #[error("No arrays were created after performing the transform")]
    EmptyStack,

    /// Unsupported operation
    #[error("Unsupported operation: {0}")]
    Unsupported(String),
}

impl<'a, I: Iterator<Item = &'a Scalar>> SingleRowTransform<'a, I> {
    pub(crate) fn new(scalars: impl IntoIterator<Item = &'a Scalar, IntoIter = I>) -> Self {
        Self {
            scalars: scalars.into_iter(),
            stack: Vec::new(),
            error: None,
        }
    }

    /// return the single-row array (or propagate Error). the top of `stack` should be our
    /// single-row struct array
    pub(crate) fn into_expr(mut self) -> Result<Expression, Error> {
        if let Some(e) = self.error {
            return Err(e);
        }

        if self.scalars.next().is_some() {
            return Err(Error::ExcessScalars);
        }

        match self.stack.pop() {
            Some(array) => Ok(array),
            None => Err(Error::EmptyStack),
        }
    }

    fn set_error(&mut self, e: Error) {
        if let Some(err) = &self.error.replace(e) {
            debug!("Overwriting error that was already set: {err}");
        }
    }

    fn check_error<T, E: Into<Error>>(&mut self, result: Result<T, E>) -> Option<T> {
        match result {
            Ok(val) => Some(val),
            Err(err) => {
                self.set_error(err.into());
                None
            }
        }
    }
}

impl<'a, T: Iterator<Item = &'a Scalar>> SchemaTransform<'a> for SingleRowTransform<'a, T> {
    fn transform_primitive(
        &mut self,
        prim_type: &'a PrimitiveType,
    ) -> Option<Cow<'a, PrimitiveType>> {
        // first always check error to terminate early if possible
        if self.error.is_some() {
            return None;
        }

        let next = self.scalars.next();
        let scalar = self.check_error(next.ok_or(Error::InsufficientScalars))?;

        let DataType::Primitive(scalar_type) = scalar.data_type() else {
            self.set_error(Error::Schema(
                "Non-primitive scalar type {datatype} provided".to_string(),
            ));
            return None;
        };
        if scalar_type != *prim_type {
            self.set_error(Error::Schema(format!(
                "Mismatched scalar type creating a single-row array: expected {}, got {}",
                prim_type, scalar_type
            )));
            return None;
        }

        self.stack.push(Expression::Literal(scalar.clone()));
        Some(Cow::Borrowed(prim_type))
    }

    fn transform_struct(&mut self, struct_type: &'a StructType) -> Option<Cow<'a, StructType>> {
        // first always check error to terminate early if possible
        if self.error.is_some() {
            return None;
        }

        // Only consume newly-added entries (if any). There could be fewer than expected if
        // the recursion encountered an error.
        let mark = self.stack.len();
        self.recurse_into_struct(struct_type)?;
        let field_exprs = self.stack.split_off(mark);

        if field_exprs.len() != struct_type.fields_len() {
            self.set_error(Error::InsufficientScalars);
            return None;
        }

        let mut found_non_nullable_null = false;
        let mut all_null = true;
        let fields = struct_type.fields();
        for (field, expr) in fields.zip(&field_exprs) {
            if !matches!(expr, Expression::Literal(Scalar::Null(_))) {
                all_null = false;
            } else if !field.is_nullable() {
                found_non_nullable_null = true;
            }
        }

        // If all children are NULL and at least one is ostensibly non-nullable, we interpret
        // the struct itself as being NULL
        let struct_expr = if all_null && found_non_nullable_null {
            Expression::null_literal(struct_type.clone().into())
        } else if found_non_nullable_null {
            // we found a non_nullable NULL, but other siblings are non-null: error
            self.set_error(Error::Schema(
                "NULL value for non-nullable struct field with non-NULL siblings".to_string(),
            ));
            return None;
        } else {
            Expression::struct_from(field_exprs)
        };

        self.stack.push(struct_expr);
        Some(Cow::Borrowed(struct_type))
    }

    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        // first always check error to terminate early if possible
        if self.error.is_some() {
            return None;
        }

        self.recurse_into_struct_field(field);
        Some(Cow::Borrowed(field))
    }

    // arrays unsupported for now
    fn transform_array(&mut self, _array_type: &'a ArrayType) -> Option<Cow<'a, ArrayType>> {
        // first always check error to terminate early if possible
        if self.error.is_some() {
            return None;
        }
        self.set_error(Error::Unsupported(
            "ArrayType not yet supported for creating single-row array".to_string(),
        ));
        None
    }

    // maps unsupported for now
    fn transform_map(&mut self, _map_type: &'a MapType) -> Option<Cow<'a, MapType>> {
        // first always check error to terminate early if possible
        if self.error.is_some() {
            return None;
        }
        self.set_error(Error::Unsupported(
            "MapType not yet supported for creating single-row array".to_string(),
        ));
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use crate::schema::SchemaRef;
    use crate::schema::StructType;
    use crate::DataType as DeltaDataTypes;

    use paste::paste;

    // helper to take values/schema to pass to `create_one` and assert the result = expected
    fn assert_single_row_transform(
        values: &[Scalar],
        schema: SchemaRef,
        expected: Result<Expression, ()>,
    ) {
        let mut schema_transform = SingleRowTransform::new(values);
        let datatype = schema.into();
        let _transformed = schema_transform.transform(&datatype);
        match expected {
            Ok(expected_expr) => {
                let actual_expr = schema_transform.into_expr().unwrap();
                // TODO: we can't compare NULLs so we convert with .to_string to workaround
                // see: https://github.com/delta-io/delta-kernel-rs/pull/677
                assert_eq!(expected_expr.to_string(), actual_expr.to_string());
            }
            Err(()) => {
                assert!(schema_transform.into_expr().is_err());
            }
        }
    }

    #[test]
    fn test_create_one_top_level_null() {
        let values = &[Scalar::Null(DeltaDataTypes::INTEGER)];

        let schema = Arc::new(StructType::new([StructField::not_null(
            "col_1",
            DeltaDataTypes::INTEGER,
        )]));
        let expected = Expression::null_literal(schema.clone().into());
        assert_single_row_transform(values, schema, Ok(expected));

        let schema = Arc::new(StructType::new([StructField::nullable(
            "col_1",
            DeltaDataTypes::INTEGER,
        )]));
        let expected =
            Expression::struct_from(vec![Expression::null_literal(DeltaDataTypes::INTEGER)]);
        assert_single_row_transform(values, schema, Ok(expected));
    }

    #[test]
    fn test_create_one_missing_values() {
        let values = &[1.into()];
        let schema = Arc::new(StructType::new([
            StructField::nullable("col_1", DeltaDataTypes::INTEGER),
            StructField::nullable("col_2", DeltaDataTypes::INTEGER),
        ]));
        assert_single_row_transform(values, schema, Err(()));
    }

    #[test]
    fn test_create_one_extra_values() {
        let values = &[1.into(), 2.into(), 3.into()];
        let schema = Arc::new(StructType::new([
            StructField::nullable("col_1", DeltaDataTypes::INTEGER),
            StructField::nullable("col_2", DeltaDataTypes::INTEGER),
        ]));
        assert_single_row_transform(values, schema, Err(()));
    }

    #[test]
    fn test_create_one_incorrect_schema() {
        let values = &["a".into()];
        let schema = Arc::new(StructType::new([StructField::nullable(
            "col_1",
            DeltaDataTypes::INTEGER,
        )]));
        assert_single_row_transform(values, schema, Err(()));
    }

    // useful test to make sure that we correctly process the stack
    #[test]
    fn test_many_structs() {
        let values: &[Scalar] = &[1.into(), 2.into(), 3.into(), 4.into()];
        let schema = Arc::new(StructType::new([
            StructField::nullable(
                "x",
                DeltaDataTypes::struct_type([
                    StructField::not_null("a", DeltaDataTypes::INTEGER),
                    StructField::nullable("b", DeltaDataTypes::INTEGER),
                ]),
            ),
            StructField::nullable(
                "y",
                DeltaDataTypes::struct_type([
                    StructField::not_null("c", DeltaDataTypes::INTEGER),
                    StructField::nullable("d", DeltaDataTypes::INTEGER),
                ]),
            ),
        ]));
        let expected = Expression::struct_from(vec![
            Expression::struct_from(vec![Expression::literal(1), Expression::literal(2)]),
            Expression::struct_from(vec![Expression::literal(3), Expression::literal(4)]),
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
            StructType::new([field_a.clone(), field_b.clone()]),
            test_schema.x_nullable,
        );
        let schema = Arc::new(StructType::new([field_x.clone()]));

        let expected_result = match expected {
            Expected::Noop => {
                let nested_struct = Expression::struct_from(vec![
                    Expression::literal(values[0].clone()),
                    Expression::literal(values[1].clone()),
                ]);
                Ok(Expression::struct_from([nested_struct]))
            }
            Expected::Null => Ok(Expression::null_literal(schema.clone().into())),
            Expected::NullStruct => {
                let nested_null = Expression::null_literal(field_x.data_type().clone());
                Ok(Expression::struct_from([nested_null]))
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
