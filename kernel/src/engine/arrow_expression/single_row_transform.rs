use std::borrow::Cow;
use std::sync::Arc;

use arrow_array::{ArrayRef, StructArray};
use arrow_buffer::NullBuffer;
use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field as ArrowField;
use itertools::Itertools;
use tracing::debug;

use crate::expressions::Scalar;
use crate::schema::{
    ArrayType, DataType, MapType, PrimitiveType, SchemaTransform, StructField, StructType,
};

/// [`SchemaTransform`] that will transform a [`Schema`] and an ordered list of leaf values (as
/// Scalar slice) into an arrow Array with a single row of each literal.
#[derive(Debug)]
pub(crate) struct SingleRowTransform<'a, T: Iterator<Item = &'a Scalar>> {
    /// Leaf-node values to insert in schema order.
    scalars: T,
    /// A stack of built arrays. After visiting children, we pop them off to
    /// build the parent container, then push the parent back on.
    stack: Vec<ArrayRef>,
    /// If an error occurs, it will be stored here.
    error: Option<Error>,
}

/// Any error for the single-row array transform.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// An error performing operations on arrow data
    #[error(transparent)]
    Arrow(#[from] arrow_schema::ArrowError),

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
    pub(crate) fn into_struct_array(mut self) -> Result<ArrayRef, Error> {
        if let Some(e) = self.error {
            return Err(e);
        }

        if self.scalars.next().is_some() {
            return Err(Error::ExcessScalars);
        }

        match self.stack.pop() {
            Some(array) if self.stack.is_empty() => match array.data_type() {
                ArrowDataType::Struct(_) => Ok(array),
                _ => Err(Error::Schema("Expected struct array".to_string())),
            },
            Some(_) => Err(Error::ExcessScalars),
            None => Err(Error::EmptyStack),
        }
    }

    fn set_error(&mut self, e: Error) {
        if let Some(err) = &self.error.replace(e) {
            debug!("Overwriting error that was already set: {err}");
        }
    }

    // TODO use check_error everywhere
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

        let arr = self.check_error(
            scalar
                .to_array(1)
                .map_err(|delta_error| Error::Schema(delta_error.to_string())),
        )?;
        self.stack.push(arr);

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
        let field_arrays = self.stack.split_off(mark);

        if field_arrays.len() != struct_type.fields_len() {
            self.set_error(Error::InsufficientScalars);
            return None;
        }

        let fields =
            self.check_error(struct_type.fields().map(ArrowField::try_from).try_collect())?;

        let mut found_non_nullable_null = false;
        let mut all_null = true;
        for (f, v) in struct_type.fields().zip(&field_arrays) {
            if v.is_valid(0) {
                all_null = false;
            } else if !f.is_nullable() {
                found_non_nullable_null = true;
            }
        }

        // If all children are NULL and at least one is ostensibly non-nullable, we must interpret
        // the struct itself as being NULL or `StructArray::try_new` will fail null checks.
        let null_buffer = (all_null && found_non_nullable_null).then(|| NullBuffer::new_null(1));
        let array = self.check_error(StructArray::try_new(fields, field_arrays, null_buffer))?;

        self.stack.push(Arc::new(array));
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

    use crate::schema::SchemaRef;
    use crate::schema::StructType;
    use crate::DataType as DeltaDataTypes;

    use arrow_array::cast::AsArray;
    use arrow_array::create_array;
    use arrow_schema::{DataType, Field};

    // helper to take values/schema to pass to `create_one` and assert the result = expected
    fn assert_single_row_transform(
        values: &[Scalar],
        schema: SchemaRef,
        expected: Result<StructArray, ()>,
    ) {
        let mut array_transform = SingleRowTransform::new(values);
        let datatype = schema.into();
        let _transformed = array_transform.transform(&datatype);
        match expected {
            Ok(expected_struct) => {
                let array = array_transform.into_struct_array().unwrap();
                let struct_array = array.as_struct_opt().unwrap();
                assert_eq!(struct_array, &expected_struct);
            }
            Err(()) => {
                assert!(array_transform.into_struct_array().is_err());
            }
        }
    }

    #[derive(Clone, Copy)]
    struct TestSchema {
        x_nullable: bool,
        a_nullable: bool,
        b_nullable: bool,
    }
    struct TestGroup {
        schema: TestSchema,
        expected: [Expected; 4],
    }
    enum Expected {
        Noop,
        NullStruct,
        Null,
        Error, // TODO we could check the actual error
    }

    impl TestGroup {
        fn run(self) {
            let value_groups = [
                (Some(1), Some(2)),
                (None, Some(2)),
                (Some(1), None),
                (None, None),
            ];
            for (i, (a_val, b_val)) in value_groups.into_iter().enumerate() {
                let a = match a_val {
                    Some(v) => Scalar::Integer(v),
                    None => Scalar::Null(DeltaDataTypes::INTEGER),
                };
                let b = match b_val {
                    Some(v) => Scalar::Integer(v),
                    None => Scalar::Null(DeltaDataTypes::INTEGER),
                };
                let values: &[Scalar] = &[a, b];
                let schema = Arc::new(StructType::new([StructField::new(
                    "x",
                    DeltaDataTypes::struct_type([
                        StructField::new("a", DeltaDataTypes::INTEGER, self.schema.a_nullable),
                        StructField::new("b", DeltaDataTypes::INTEGER, self.schema.b_nullable),
                    ]),
                    self.schema.x_nullable,
                )]));

                let struct_fields = vec![
                    Field::new("a", DataType::Int32, self.schema.a_nullable),
                    Field::new("b", DataType::Int32, self.schema.b_nullable),
                ];

                let field_a = Arc::new(Field::new("a", DataType::Int32, self.schema.a_nullable));
                let field_b = Arc::new(Field::new("b", DataType::Int32, self.schema.b_nullable));
                let expected = match self.expected[i] {
                    Expected::Noop => Ok(StructArray::from(vec![(
                        Arc::new(Field::new(
                            "x",
                            DataType::Struct(struct_fields.into()),
                            self.schema.x_nullable,
                        )),
                        Arc::new(StructArray::from(vec![
                            (field_a, create_array!(Int32, [a_val]) as ArrayRef),
                            (field_b, create_array!(Int32, [b_val]) as ArrayRef),
                        ])) as ArrayRef,
                    )])),
                    Expected::Null => Ok(StructArray::new_null(
                        vec![Field::new(
                            "x",
                            DataType::Struct(struct_fields.into()),
                            self.schema.x_nullable,
                        )]
                        .into(),
                        1,
                    )),
                    Expected::NullStruct => Ok(StructArray::from(vec![(
                        Arc::new(Field::new(
                            "x",
                            DataType::Struct(struct_fields.into()),
                            self.schema.x_nullable,
                        )),
                        Arc::new(StructArray::new_null(
                            vec![field_a.clone(), field_b.clone()].into(),
                            1,
                        )) as ArrayRef,
                    )])),
                    Expected::Error => Err(()),
                };
                assert_single_row_transform(values, schema, expected);
            }
        }
    }

    // test cases: x, a, b are nullable (n) or not-null (!). we have 6 interesting nullability
    // combinations:
    // 1. n { n, n }
    // 2. n { n, ! }
    // 3. n { !, ! }
    // 4. ! { n, n }
    // 5. ! { n, ! }
    // 6. ! { !, ! }
    //
    // and for each we want to test the four combinations of values ("a" and "b" just chosen as
    // abitrary scalars):
    // 1. (a, b)
    // 2. (N, b)
    // 3. (a, N)
    // 4. (N, N)
    //
    // here's the full list of test cases with expected output:
    //
    // n { n, n }
    // 1. (a, b) -> x (a, b)
    // 2. (N, b) -> x (N, b)
    // 3. (a, N) -> x (a, N)
    // 4. (N, N) -> x (N, N)
    //
    // n { n, ! }
    // 1. (a, b) -> x (a, b)
    // 2. (N, b) -> x (N, b)
    // 3. (a, N) -> Err
    // 4. (N, N) -> x NULL
    //
    // n { !, ! }
    // 1. (a, b) -> x (a, b)
    // 2. (N, b) -> Err
    // 3. (a, N) -> Err
    // 4. (N, N) -> x NULL
    //
    // ! { n, n }
    // 1. (a, b) -> x (a, b)
    // 2. (N, b) -> x (N, b)
    // 3. (a, N) -> x (a, N)
    // 4. (N, N) -> x (N, N)
    //
    // ! { n, ! }
    // 1. (a, b) -> x (a, b)
    // 2. (N, b) -> x (N, b)
    // 3. (a, N) -> Err
    // 4. (N, N) -> NULL
    //
    // ! { !, ! }
    // 1. (a, b) -> x (a, b)
    // 2. (N, b) -> Err
    // 3. (a, N) -> Err
    // 4. (N, N) -> NULL

    // A helper macro to map the nullability specification to booleans
    macro_rules! bool_from_nullable {
        (nullable) => {
            true
        };
        (not_null) => {
            false
        };
    }

    // The main macro: for each test case we list a test name, the values for x, a, b, and the expected outcomes.
    macro_rules! test_nullability_combinations {
    (
        $(
            $test_name:ident: {
                x: $x:ident,
                a: $a:ident,
                b: $b:ident,
                expected: [$($expected:expr),+ $(,)?]
            }
        ),+ $(,)?
    ) => {
        $(
            #[test]
            fn $test_name() {
                let test_group = TestGroup {
                    schema: TestSchema {
                        x_nullable: bool_from_nullable!($x),
                        a_nullable: bool_from_nullable!($a),
                        b_nullable: bool_from_nullable!($b),
                    },
                    expected: [$($expected),+],
                };
                test_group.run();
            }
        )+
        };
    }

    test_nullability_combinations! {
        // n { n, n } case: all nullable
        test_all_nullable: {
            x: nullable,
            a: nullable,
            b: nullable,
            expected: [
                Expected::Noop,
                Expected::Noop,
                Expected::Noop,
                Expected::Noop,
            ]
        },
        // n { n, ! } case: x and a are nullable, b is not-null
        test_nullable_nullable_not_null: {
            x: nullable,
            a: nullable,
            b: not_null,
            expected: [
                Expected::Noop,
                Expected::Noop,
                Expected::Error,
                Expected::NullStruct,
            ]
        },
        // n { !, ! } case: x is nullable; a and b are not-null
        test_nullable_not_null_not_null: {
            x: nullable,
            a: not_null,
            b: not_null,
            expected: [
                Expected::Noop,
                Expected::Error,
                Expected::Error,
                Expected::NullStruct,
            ]
        },
        // ! { n, n } case: x is not-null; a and b are nullable
        test_not_null_nullable_nullable: {
            x: not_null,
            a: nullable,
            b: nullable,
            expected: [
                Expected::Noop,
                Expected::Noop,
                Expected::Noop,
                Expected::Noop,
            ]
        },
        // ! { n, ! } case: x is not-null; a is nullable, b is not-null
        test_not_null_nullable_not_null: {
            x: not_null,
            a: nullable,
            b: not_null,
            expected: [
                Expected::Noop,
                Expected::Noop,
                Expected::Error,
                Expected::Null,
            ]
        },
        // ! { !, ! } case: all not-null (for a and b)
        test_not_null_not_null_not_null: {
            x: not_null,
            a: not_null,
            b: not_null,
            expected: [
                Expected::Noop,
                Expected::Error,
                Expected::Error,
                Expected::Null,
            ]
        },
    }
}
