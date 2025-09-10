//! The [`LiteralExpressionTransform`] is a [`SchemaTransform`] that transforms a [`Schema`] and an
//! ordered list of leaf values (scalars) into an [`Expression`] with a literal value for each leaf.

use crate::expressions::{Expression, Scalar};
use crate::schema::{ArrayType, DataType, MapType, PrimitiveType, StructField, StructType};
use crate::DeltaResult;

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

#[derive(Debug, Default)]
pub(crate) struct LiteralExpressionTransform<'a, T: Iterator<Item = &'a Scalar>> {
    /// Leaf values to insert in schema order.
    scalars: T,
}

impl<'a, I: Iterator<Item = &'a Scalar>> LiteralExpressionTransform<'a, I> {
    pub(crate) fn new(scalars: impl IntoIterator<IntoIter = I>) -> Self {
        Self {
            scalars: scalars.into_iter(),
        }
    }

    /// Bind the visitor to a StructType and produce an Expression
    pub(crate) fn bind(&mut self, struct_type: &StructType) -> DeltaResult<Expression> {
        use crate::schema::visitor::visit_struct;
        let result = visit_struct(struct_type, self)?;

        // Check for excess scalars after visiting
        if let Some(scalar) = self.scalars.next() {
            return Err(Error::ExcessScalars(scalar.clone()).into());
        }

        Ok(result)
    }

    fn visit_leaf(&mut self, schema_type: &DataType) -> DeltaResult<Expression> {
        let Some(scalar) = self.scalars.next() else {
            return Err(Error::InsufficientScalars.into());
        };

        if schema_type.clone() != scalar.data_type() {
            return Err(Error::Schema(format!(
                "Mismatched scalar type while creating Expression: expected {:?}, got {:?}",
                schema_type,
                scalar.data_type()
            ))
            .into());
        };

        Ok(Expression::Literal(scalar.clone()))
    }
}

impl<'a, I: Iterator<Item = &'a Scalar>> delta_kernel::schema::visitor::SchemaVisitor
    for LiteralExpressionTransform<'a, I>
{
    type T = Expression;

    fn field(&mut self, field: &StructField, value: Self::T) -> DeltaResult<Self::T> {
        match &field.data_type {
            DataType::Struct(_) => Ok(value),
            DataType::Primitive(_) => self.visit_leaf(&field.data_type),
            DataType::Array(_) => self.visit_leaf(&field.data_type),
            DataType::Map(_) => self.visit_leaf(&field.data_type),
            DataType::Variant(_) => self.visit_leaf(&field.data_type),
        }
    }

    fn r#struct(
        &mut self,
        struct_type: &StructType,
        field_exprs: Vec<Self::T>,
    ) -> DeltaResult<Self::T> {
        let fields = struct_type.fields();
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
                return Err(Error::Schema(
                    "NULL value for non-nullable struct field with non-NULL siblings".to_string(),
                )
                .into());
            }
            Expression::null_literal(struct_type.clone().into())
        } else {
            Expression::struct_from(field_exprs)
        };

        Ok(struct_expr)
    }

    fn list(&mut self, _list: &ArrayType, _value: Self::T) -> DeltaResult<Self::T> {
        // Everything is handled on the field level
        Ok(Expression::Unknown("Should not happen".to_string()))
    }

    fn map(
        &mut self,
        _map: &MapType,
        _key_value: Self::T,
        _value: Self::T,
    ) -> DeltaResult<Self::T> {
        // Everything is handled on the field level
        Ok(Expression::Unknown("Should not happen".to_string()))
    }

    fn primitive(&mut self, _p: &PrimitiveType) -> DeltaResult<Self::T> {
        // Everything is handled on the field level
        Ok(Expression::Unknown("Should not happen".to_string()))
    }

    fn variant(&mut self, _struct: &StructType) -> DeltaResult<Self::T> {
        // Everything is handled on the field level
        Ok(Expression::Unknown("Should not happen".to_string()))
    }
}

struct StructFieldIterator {
    stack: Vec<StructType>,
    pos: Vec<usize>,
}

impl StructFieldIterator {
    fn new(root: StructType) -> Self {
        StructFieldIterator {
            stack: vec![root],
            pos: vec![0],
        }
    }
}

impl Iterator for StructFieldIterator {
    type Item = StructField;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(level) = self.stack.last().cloned() {
            let current_position = *self.pos.last().unwrap();

            if current_position < level.fields_len() {
                let field = level.by_index(current_position).clone();

                // Increment position for next iteration
                *self.pos.last_mut().unwrap() += 1;

                // If this field is a nested struct, push it onto the stack
                if let DataType::Struct(nested_struct) = &field.data_type {
                    self.stack.push(nested_struct.as_ref().clone());
                    self.pos.push(0);
                }

                return Some(field);
            } else {
                // Current level exhausted, pop it
                self.stack.pop();
                self.pos.pop();
            }
        }
        None
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
        let actual = LiteralExpressionTransform::new(values).bind(&schema);
        match expected {
            Ok(expected_expr) => {
                // TODO: we can't compare NULLs so we convert with .to_string to workaround
                // see: https://github.com/delta-io/delta-kernel-rs/pull/1267
                assert_eq!(expected_expr.to_string(), actual.unwrap().to_string());
            }
            Err(()) => {
                assert!(actual.is_err());
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
        let expected = Expr::null_literal(schema.clone().into());
        assert_single_row_transform(values, schema, Ok(expected));

        let schema = Arc::new(StructType::new([StructField::nullable(
            "col_1",
            DeltaDataTypes::INTEGER,
        )]));
        let expected = Expr::struct_from(vec![Expr::null_literal(DeltaDataTypes::INTEGER)]);
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
        let schema = Arc::new(StructType::new([
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
            StructType::new([field_a.clone(), field_b.clone()]),
            test_schema.x_nullable,
        );
        let schema = Arc::new(StructType::new([field_x.clone()]));

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

    #[cfg(test)]
    mod struct_field_iterator_tests {
        use super::*;
        use crate::DataType as DeltaDataTypes;

        #[test]
        fn test_simple_flat_struct() {
            let schema = StructType::new([
                StructField::nullable("field1", DeltaDataTypes::INTEGER),
                StructField::not_null("field2", DeltaDataTypes::STRING),
                StructField::nullable("field3", DeltaDataTypes::BOOLEAN),
            ]);

            let iterator = StructFieldIterator::new(schema);
            let fields: Vec<StructField> = iterator.collect();

            assert_eq!(fields.len(), 3);
            assert_eq!(fields[0].name(), "field1");
            assert_eq!(fields[0].data_type(), &DeltaDataTypes::INTEGER);
            assert_eq!(fields[1].name(), "field2");
            assert_eq!(fields[1].data_type(), &DeltaDataTypes::STRING);
            assert_eq!(fields[2].name(), "field3");
            assert_eq!(fields[2].data_type(), &DeltaDataTypes::BOOLEAN);
        }

        #[test]
        fn test_nested_struct() {
            let inner_struct = StructType::new([
                StructField::nullable("inner1", DeltaDataTypes::INTEGER),
                StructField::nullable("inner2", DeltaDataTypes::STRING),
            ]);

            let schema = StructType::new([
                StructField::nullable("outer1", DeltaDataTypes::INTEGER),
                StructField::nullable("nested", inner_struct.clone()),
                StructField::nullable("outer2", DeltaDataTypes::STRING),
            ]);

            let iterator = StructFieldIterator::new(schema);
            let fields: Vec<StructField> = iterator.collect();

            // Should get: outer1, nested, inner1, inner2, outer2
            assert_eq!(fields.len(), 5);
            assert_eq!(fields[0].name(), "outer1");
            assert_eq!(fields[0].data_type(), &DeltaDataTypes::INTEGER);
            assert_eq!(fields[1].name(), "nested");
            assert!(matches!(fields[1].data_type(), DataType::Struct(_)));
            assert_eq!(fields[2].name(), "inner1");
            assert_eq!(fields[2].data_type(), &DeltaDataTypes::INTEGER);
            assert_eq!(fields[3].name(), "inner2");
            assert_eq!(fields[3].data_type(), &DeltaDataTypes::STRING);
            assert_eq!(fields[4].name(), "outer2");
            assert_eq!(fields[4].data_type(), &DeltaDataTypes::STRING);
        }

        #[test]
        fn test_deeply_nested_structs() {
            let level2 = StructType::new([StructField::nullable(
                "level2_field",
                DeltaDataTypes::INTEGER,
            )]);

            let level1 = StructType::new([
                StructField::nullable("level1_field", DeltaDataTypes::STRING),
                StructField::nullable("level2", level2),
            ]);

            let schema = StructType::new([
                StructField::nullable("root_field", DeltaDataTypes::BOOLEAN),
                StructField::nullable("level1", level1),
            ]);

            let iterator = StructFieldIterator::new(schema);
            let fields: Vec<StructField> = iterator.collect();

            // Should get: root_field, level1, level1_field, level2, level2_field
            assert_eq!(fields.len(), 5);
            assert_eq!(fields[0].name(), "root_field");
            assert_eq!(fields[1].name(), "level1");
            assert_eq!(fields[2].name(), "level1_field");
            assert_eq!(fields[3].name(), "level2");
            assert_eq!(fields[4].name(), "level2_field");
        }

        #[test]
        fn test_struct_with_array_types() {
            let array_type = ArrayType::new(DeltaDataTypes::INTEGER, true);
            let schema = StructType::new([
                StructField::nullable("before_array", DeltaDataTypes::STRING),
                StructField::nullable(
                    "array_field",
                    DeltaDataTypes::Array(Box::new(array_type.clone())),
                ),
                StructField::nullable("after_array", DeltaDataTypes::BOOLEAN),
            ]);

            let iterator = StructFieldIterator::new(schema);
            let fields: Vec<StructField> = iterator.collect();

            assert_eq!(fields.len(), 3);
            assert_eq!(fields[0].name(), "before_array");
            assert_eq!(fields[0].data_type(), &DeltaDataTypes::STRING);
            assert_eq!(fields[1].name(), "array_field");
            assert!(matches!(fields[1].data_type(), DataType::Array(_)));
            assert_eq!(fields[2].name(), "after_array");
            assert_eq!(fields[2].data_type(), &DeltaDataTypes::BOOLEAN);
        }

        #[test]
        fn test_struct_with_map_types() {
            let map_type = MapType::new(DeltaDataTypes::STRING, DeltaDataTypes::INTEGER, false);
            let schema = StructType::new([
                StructField::nullable("before_map", DeltaDataTypes::STRING),
                StructField::nullable("map_field", DeltaDataTypes::Map(Box::new(map_type.clone()))),
                StructField::nullable("after_map", DeltaDataTypes::BOOLEAN),
            ]);

            let iterator = StructFieldIterator::new(schema);
            let fields: Vec<StructField> = iterator.collect();

            assert_eq!(fields.len(), 3);
            assert_eq!(fields[0].name(), "before_map");
            assert_eq!(fields[0].data_type(), &DeltaDataTypes::STRING);
            assert_eq!(fields[1].name(), "map_field");
            assert!(matches!(fields[1].data_type(), DataType::Map(_)));
            assert_eq!(fields[2].name(), "after_map");
            assert_eq!(fields[2].data_type(), &DeltaDataTypes::BOOLEAN);
        }

        #[test]
        fn test_complex_mixed_structure() {
            // Create a complex structure with nested structs, arrays, and maps
            let array_type = ArrayType::new(DeltaDataTypes::STRING, true);
            let map_type = MapType::new(DeltaDataTypes::STRING, DeltaDataTypes::INTEGER, false);

            let inner_struct = StructType::new([
                StructField::nullable("inner_primitive", DeltaDataTypes::DOUBLE),
                StructField::nullable(
                    "inner_array",
                    DeltaDataTypes::Array(Box::new(array_type.clone())),
                ),
            ]);

            let schema = StructType::new([
                StructField::nullable("root_string", DeltaDataTypes::STRING),
                StructField::nullable("root_map", DeltaDataTypes::Map(Box::new(map_type.clone()))),
                StructField::nullable("nested_struct", inner_struct),
                StructField::nullable("root_array", DeltaDataTypes::Array(Box::new(array_type))),
                StructField::nullable("root_int", DeltaDataTypes::INTEGER),
            ]);

            let iterator = StructFieldIterator::new(schema);
            let fields: Vec<StructField> = iterator.collect();

            // Should get: root_string, root_map, nested_struct, inner_primitive, inner_array, root_array, root_int
            assert_eq!(fields.len(), 7);

            let field_names: Vec<&str> = fields.iter().map(|f| f.name().as_str()).collect();
            assert_eq!(
                field_names,
                vec![
                    "root_string",
                    "root_map",
                    "nested_struct",
                    "inner_primitive",
                    "inner_array",
                    "root_array",
                    "root_int"
                ]
            );

            // Verify data types
            assert_eq!(fields[0].data_type(), &DeltaDataTypes::STRING);
            assert!(matches!(fields[1].data_type(), DataType::Map(_)));
            assert!(matches!(fields[2].data_type(), DataType::Struct(_)));
            assert_eq!(fields[3].data_type(), &DeltaDataTypes::DOUBLE);
            assert!(matches!(fields[4].data_type(), DataType::Array(_)));
            assert!(matches!(fields[5].data_type(), DataType::Array(_)));
            assert_eq!(fields[6].data_type(), &DeltaDataTypes::INTEGER);
        }

        #[test]
        fn test_empty_struct() {
            let schema = StructType::new([]);
            let iterator = StructFieldIterator::new(schema);
            let fields: Vec<StructField> = iterator.collect();
            assert_eq!(fields.len(), 0);
        }

        #[test]
        fn test_struct_with_nested_empty_struct() {
            let empty_struct = StructType::new([]);
            let schema = StructType::new([
                StructField::nullable("before_empty", DeltaDataTypes::STRING),
                StructField::nullable("empty_nested", empty_struct),
                StructField::nullable("after_empty", DeltaDataTypes::INTEGER),
            ]);

            let iterator = StructFieldIterator::new(schema);
            let fields: Vec<StructField> = iterator.collect();

            // Should get: before_empty, empty_nested, after_empty
            assert_eq!(fields.len(), 3);
            assert_eq!(fields[0].name(), "before_empty");
            assert_eq!(fields[1].name(), "empty_nested");
            assert_eq!(fields[2].name(), "after_empty");
        }

        #[test]
        fn test_multiple_sibling_nested_structs() {
            let struct1 = StructType::new([StructField::nullable(
                "struct1_field",
                DeltaDataTypes::STRING,
            )]);

            let struct2 = StructType::new([
                StructField::nullable("struct2_field1", DeltaDataTypes::INTEGER),
                StructField::nullable("struct2_field2", DeltaDataTypes::BOOLEAN),
            ]);

            let schema = StructType::new([
                StructField::nullable("root_field", DeltaDataTypes::DOUBLE),
                StructField::nullable("first_nested", struct1),
                StructField::nullable("second_nested", struct2),
                StructField::nullable("final_field", DeltaDataTypes::STRING),
            ]);

            let iterator = StructFieldIterator::new(schema);
            let fields: Vec<StructField> = iterator.collect();

            // Should get: root_field, first_nested, struct1_field, second_nested, struct2_field1, struct2_field2, final_field
            assert_eq!(fields.len(), 7);

            let field_names: Vec<&str> = fields.iter().map(|f| f.name().as_str()).collect();
            assert_eq!(
                field_names,
                vec![
                    "root_field",
                    "first_nested",
                    "struct1_field",
                    "second_nested",
                    "struct2_field1",
                    "struct2_field2",
                    "final_field"
                ]
            );
        }
    }
}
