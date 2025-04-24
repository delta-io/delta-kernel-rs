use std::cmp::Ordering;
use std::sync::Arc;

use crate::arrow::array::cast::AsArray;
use crate::arrow::array::{types::*, ArrowNumericType, PrimitiveArray};
use crate::arrow::array::{Array, ArrayRef, BooleanArray, GenericListArray, RecordBatch};
use crate::arrow::compute::cast;
use crate::arrow::compute::{in_list, in_list_utf8};
use crate::arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
use paste::paste;

use super::evaluate_expression::{evaluate_expression, extract_column};
use crate::error::{DeltaResult, Error};
use crate::expressions::{ArrayData, Expression, JunctionOperator, Scalar};
use crate::kernel_predicates::KernelPredicateEvaluatorDefaults;
use crate::schema::PrimitiveType;

/// Evaluates an "IN" expression between a value and a list.
///
/// This function handles various combinations of literal values and columns:
/// - When the left side is NULL, returns NULL for each row abd avoids evaluations.
/// - When both sides are literals, evaluates once and replicates the result for each row.
/// - When the right side is a list column, checks if each left value exists in the corresponding list
///
/// Returns a boolean array indicating whether each left value is found in the corresponding list.
pub(super) fn eval_in_list(
    batch: &RecordBatch,
    left: &Expression,
    right: &Expression,
) -> Result<ArrayRef, Error> {
    use Expression::*;

    let result = match (left, right) {
        (Literal(Scalar::Null(_)), Column(_) | Literal(Scalar::Array(_))) => {
            // Searching any in-list for NULL always returns NULL -- no need to actually search
            BooleanArray::new_null(batch.num_rows())
        }
        (Literal(lit), Literal(Scalar::Array(ad))) => {
            // Search the literal in-list once and then replicate the returned single-row result
            let exists = is_in_list(ad, Some(Some(lit.clone()))).iter().next();
            BooleanArray::from(vec![exists.flatten(); batch.num_rows()])
        }
        (Literal(_) | Column(_), Column(_)) => lit_or_col_in_col(batch, left, right)?,
        (Column(name), Literal(Scalar::Array(ad))) => col_in_lit(batch, name, ad)?,
        (l, r) => {
            return Err(Error::invalid_expression(format!(
                "Invalid right value for (NOT) IN comparison, left is: {l} right is: {r}"
            )));
        }
    };
    Ok(Arc::new(result))
}

/// Evaluates an "IN" expression between a column and a list.
fn col_in_lit(
    batch: &RecordBatch,
    name: &crate::schema::ColumnName,
    ad: &ArrayData,
) -> Result<BooleanArray, Error> {
    fn scalars_from<'a>(
        values: impl IntoIterator<Item = Option<impl Into<Scalar>>> + 'a,
    ) -> impl IntoIterator<Item = Option<Scalar>> + 'a {
        values.into_iter().map(|v| v.map(Into::into))
    }
    fn to_scalars<T: ArrowPrimitiveType>(
        values: &PrimitiveArray<T>,
        from: fn(T::Native) -> Scalar,
    ) -> impl IntoIterator<Item = Option<Scalar>> + '_ {
        values.iter().map(move |v| v.map(from))
    }
    let column = extract_column(batch, name)?;
    let data_type = ad
        .array_type()
        .element_type()
        .as_primitive_opt()
        .ok_or_else(|| {
            Error::invalid_expression(format!(
                "IN only supports array literals with primitive elements, got: '{:?}'",
                ad.array_type().element_type()
            ))
        })?;
    macro_rules! column_as {
        ($what: ident $(:: < $t: ty >)? ) => {
            paste! { column.[<as_ $what _opt>] $(::<$t>)? () }
                .ok_or(Error::invalid_expression(format!(
                    "Cannot cast {} to {}", column.data_type(), data_type
                )))
        };
    }
    Ok(match (column.data_type(), data_type) {
        (ArrowDataType::Utf8, PrimitiveType::String) => {
            is_in_list(ad, scalars_from(column_as!(string::<i32>)?))
        }
        (ArrowDataType::LargeUtf8, PrimitiveType::String) => {
            is_in_list(ad, scalars_from(column_as!(string::<i64>)?))
        }
        (ArrowDataType::Utf8View, PrimitiveType::String) => {
            is_in_list(ad, scalars_from(column_as!(string_view)?))
        }
        (ArrowDataType::Boolean, PrimitiveType::Boolean) => {
            is_in_list(ad, scalars_from(column_as!(boolean)?))
        }
        (ArrowDataType::Binary, PrimitiveType::Binary) => {
            is_in_list(ad, scalars_from(column_as!(binary::<i32>)?))
        }
        (ArrowDataType::LargeBinary, PrimitiveType::Binary) => {
            is_in_list(ad, scalars_from(column_as!(binary::<i64>)?))
        }
        (ArrowDataType::BinaryView, PrimitiveType::Binary) => {
            is_in_list(ad, scalars_from(column_as!(binary_view)?))
        }
        (_, PrimitiveType::Byte) => {
            is_in_list(ad, scalars_from(column_as!(primitive::<Int8Type>)?))
        }
        (_, PrimitiveType::Short) => {
            is_in_list(ad, scalars_from(column_as!(primitive::<Int16Type>)?))
        }
        (_, PrimitiveType::Integer) => {
            is_in_list(ad, scalars_from(column_as!(primitive::<Int32Type>)?))
        }
        (_, PrimitiveType::Long) => {
            is_in_list(ad, scalars_from(column_as!(primitive::<Int64Type>)?))
        }
        (_, PrimitiveType::Float) => {
            is_in_list(ad, scalars_from(column_as!(primitive::<Float32Type>)?))
        }
        (_, PrimitiveType::Double) => {
            is_in_list(ad, scalars_from(column_as!(primitive::<Float64Type>)?))
        }
        (_, PrimitiveType::Date) => is_in_list(
            ad,
            to_scalars(column_as!(primitive::<Date32Type>)?, Scalar::Date),
        ),
        (ArrowDataType::Timestamp(TimeUnit::Microsecond, Some(_)), PrimitiveType::Timestamp) => {
            let column = column_as!(primitive::<TimestampMicrosecondType>)?;
            is_in_list(ad, to_scalars(column, Scalar::Timestamp))
        }
        (ArrowDataType::Timestamp(TimeUnit::Microsecond, None), PrimitiveType::TimestampNtz) => {
            is_in_list(
                ad,
                to_scalars(
                    column_as!(primitive::<TimestampMicrosecondType>)?,
                    Scalar::TimestampNtz,
                ),
            )
        }
        (l, r) => {
            return Err(Error::invalid_expression(format!(
                "Cannot check if value of type '{l}' is in array with value type '{r}'"
            )))
        }
    })
}

/// Evaluates an "IN" expression between a literal value or column and a list column.
fn lit_or_col_in_col(
    batch: &RecordBatch,
    left: &Expression,
    right: &Expression,
) -> Result<BooleanArray, Error> {
    let right_arr = evaluate_expression(right, batch, None)?;
    let list_field = match right_arr.data_type() {
        ArrowDataType::List(list_field) => list_field,
        ArrowDataType::LargeList(list_field) => list_field,
        // TODO: LargeListView - not fully supported by arrow yet
        _ => {
            return Err(Error::invalid_expression(format!(
                "Expected right hand side to be list column, got: {:?}",
                right_arr.data_type()
            )));
        }
    };
    let left_arr = evaluate_expression(left, batch, Some(&list_field.data_type().try_into()?))?;
    let left_arr = if left_arr.data_type() == list_field.data_type() {
        left_arr
    } else {
        cast(left_arr.as_ref(), list_field.data_type()).map_err(Error::generic_err)?
    };
    macro_rules! left_as {
        ($t: ty ) => {
            left_arr.as_primitive_opt::<$t>().ok_or_else(|| {
                Error::invalid_expression(format!(
                    "Cannot cast {} to {}",
                    left_arr.data_type(),
                    <$t>::DATA_TYPE
                ))
            })
        };
    }
    Ok(match list_field.data_type() {
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8
            if right_arr.as_list_opt::<i32>().is_some() =>
        {
            eval_arrow_utf8(&left_arr, right_arr.as_list::<i32>())?
        }
        ArrowDataType::Int8 => eval_arrow(left_as!(Int8Type)?, &right_arr)?,
        ArrowDataType::Int16 => eval_arrow(left_as!(Int16Type)?, &right_arr)?,
        ArrowDataType::Int32 => eval_arrow(left_as!(Int32Type)?, &right_arr)?,
        ArrowDataType::Int64 => eval_arrow(left_as!(Int64Type)?, &right_arr)?,
        ArrowDataType::Float16 => eval_arrow(left_as!(Float16Type)?, &right_arr)?,
        ArrowDataType::Float32 => eval_arrow(left_as!(Float32Type)?, &right_arr)?,
        ArrowDataType::Float64 => eval_arrow(left_as!(Float64Type)?, &right_arr)?,
        ArrowDataType::Timestamp(TimeUnit::Microsecond, _) => {
            eval_arrow(left_as!(TimestampMicrosecondType)?, &right_arr)?
        }
        ArrowDataType::Date32 => eval_arrow(left_as!(Date32Type)?, &right_arr)?,
        ArrowDataType::Decimal128(_, _) => eval_arrow(left_as!(Decimal128Type)?, &right_arr)?,
        _ => {
            return Err(Error::invalid_expression(format!(
                "Cannot compare {} to {}",
                left_arr.data_type(),
                list_field.data_type()
            )))
        }
    })
}

fn is_in_list(ad: &ArrayData, values: impl IntoIterator<Item = Option<Scalar>>) -> BooleanArray {
    #[allow(deprecated)]
    let inlist = ad.array_elements();
    // `v IN (k1, ..., kN)` is logically equivalent to `v = k1 OR ... OR v = kN`, so evaluate
    // it as such, ensuring correct handling of NULL inputs (including `Scalar::Null`).
    values
        .into_iter()
        .map(|v| {
            KernelPredicateEvaluatorDefaults::finish_eval_junction(
                JunctionOperator::Or,
                inlist
                    .iter()
                    .map(|k| Some(v.as_ref()?.partial_cmp(k)? == Ordering::Equal)),
                false,
            )
        })
        .collect()
}

// helper function to make arrow in_list* kernel results comliant with SQL NULL semantics.
// Specifically, if an item is not found in the in-list, but the in-list contains NULLs, the
// result should be NULL (UNKNOWN) as well.
fn fix_arrow_in_list_result(
    results: BooleanArray,
    in_lists: impl IntoIterator<Item = Option<ArrayRef>>,
) -> BooleanArray {
    results
        .iter()
        .zip(in_lists)
        .map(|(res, arr)| match (res, arr) {
            (Some(false), Some(arr)) if arr.null_count() > 0 => None,
            _ => res,
        })
        .collect()
}

fn eval_arrow_utf8(
    left_arr: &dyn Array,
    right_arr: &GenericListArray<i32>,
) -> Result<BooleanArray, Error> {
    let result = match left_arr.data_type() {
        ArrowDataType::Utf8 => in_list_utf8(left_arr.as_string::<i32>(), right_arr),
        ArrowDataType::LargeUtf8 => in_list_utf8(left_arr.as_string::<i64>(), right_arr),
        _ => return Err(Error::invalid_expression("Expected string column")),
    }
    .map_err(Error::generic_err)?;
    Ok(fix_arrow_in_list_result(result, right_arr.iter()))
}

fn eval_arrow<T: ArrowNumericType>(
    left: &PrimitiveArray<T>,
    right: &dyn Array,
) -> DeltaResult<BooleanArray> {
    match right.data_type() {
        ArrowDataType::List(_) => {
            let list_arr = right.as_list::<i32>();
            Ok(fix_arrow_in_list_result(
                in_list(left, list_arr).map_err(Error::generic_err)?,
                list_arr.iter(),
            ))
        }
        ArrowDataType::LargeList(_) => {
            let list_arr = right.as_list::<i64>();
            Ok(fix_arrow_in_list_result(
                in_list(left, list_arr).map_err(Error::generic_err)?,
                list_arr.iter(),
            ))
        }
        // TODO: LargeListView - not fully supported by arrow yet
        data_type => Err(Error::invalid_expression(format!(
            "Expected right hand side to be list column, got: {:?}",
            data_type
        ))),
    }
}

#[cfg(test)]
mod tests {
    use crate::arrow::array::{
        BinaryArray, BinaryViewArray, Date32Array, Float32Array, Float64Array, Int16Array,
        Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeListArray, LargeStringArray,
        ListArray, StringArray, StringViewArray, TimestampMicrosecondArray,
    };
    use crate::arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use crate::arrow::datatypes::Field;

    use super::*;
    use crate::{
        expressions::column_expr,
        schema::{ArrayType, DataType},
    };

    #[test]
    fn test_fix_arrow_in_list_result() {
        let in_lists = [
            Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef,
            Arc::new(Int32Array::from(vec![Some(1), Some(2)])) as ArrayRef,
        ]
        .map(Some);

        let results = BooleanArray::from(vec![Some(false), Some(false)]);
        let expected = BooleanArray::from(vec![None, Some(false)]);
        assert_eq!(
            fix_arrow_in_list_result(results, in_lists.clone()),
            expected
        );

        let results = BooleanArray::from(vec![Some(true), Some(true)]);
        let expected = BooleanArray::from(vec![Some(true), Some(true)]);
        assert_eq!(fix_arrow_in_list_result(results, in_lists), expected);
    }

    #[test]
    fn test_is_in_list() {
        let values = [
            Some(Scalar::Integer(1)),
            Some(Scalar::Integer(3)),
            Some(Scalar::Null(DataType::INTEGER)),
        ];

        let in_list = ArrayData::new(
            ArrayType::new(DataType::INTEGER, false),
            [Scalar::Integer(1), Scalar::Integer(2)],
        );
        let expected = BooleanArray::from(vec![Some(true), Some(false), None]);
        assert_eq!(is_in_list(&in_list, values.clone()), expected);

        let in_list = ArrayData::new(
            ArrayType::new(DataType::INTEGER, true),
            [Scalar::Integer(1), Scalar::Null(DataType::INTEGER)],
        );
        let expected = BooleanArray::from(vec![Some(true), None, None]);
        assert_eq!(is_in_list(&in_list, values), expected);
    }

    #[test]
    fn test_eval_in_list_lit_in_col_large() {
        let values = Int32Array::from(vec![Some(1), None, Some(2), None, Some(1), Some(2)]);
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 4, 6]));
        let field = Arc::new(Field::new("item", ArrowDataType::Int32, true));
        let arr_field = Arc::new(Field::new(
            "item",
            ArrowDataType::LargeList(field.clone()),
            true,
        ));
        let schema = crate::arrow::datatypes::Schema::new([arr_field.clone()]);
        let array = LargeListArray::new(field.clone(), offsets, Arc::new(values), None);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array.clone())]).unwrap();

        let result = eval_in_list(
            &batch,
            &Expression::literal(Scalar::Integer(1)),
            &column_expr!("item"),
        )
        .unwrap();
        let expected = Arc::new(BooleanArray::from(vec![Some(true), None, Some(true)]));
        assert_eq!(result.as_ref(), expected.as_ref());

        let result = eval_in_list(
            &batch,
            &Expression::literal(Scalar::Integer(3)),
            &column_expr!("item"),
        )
        .unwrap();
        let expected = Arc::new(BooleanArray::from(vec![None, None, Some(false)]));
        assert_eq!(result.as_ref(), expected.as_ref());
    }

    #[test]
    fn test_eval_in_list() {
        let cases = [
            (
                DataType::STRING,
                Arc::new(StringArray::from(vec![Some("one"), None, Some("three")])) as ArrayRef,
                Scalar::from("one"),
            ),
            (
                DataType::STRING,
                Arc::new(LargeStringArray::from(vec![
                    Some("one"),
                    None,
                    Some("three"),
                ])),
                Scalar::from("one"),
            ),
            (
                DataType::STRING,
                Arc::new(StringViewArray::from(vec![
                    Some("one"),
                    None,
                    Some("three"),
                ])),
                Scalar::from("one"),
            ),
            (
                DataType::INTEGER,
                Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
                Scalar::Integer(1),
            ),
            (
                DataType::SHORT,
                Arc::new(Int16Array::from(vec![Some(1), None, Some(3)])),
                Scalar::Short(1),
            ),
            (
                DataType::LONG,
                Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
                Scalar::Long(1),
            ),
            (
                DataType::BYTE,
                Arc::new(Int8Array::from(vec![Some(1), None, Some(3)])),
                Scalar::Byte(1),
            ),
            (
                DataType::FLOAT,
                Arc::new(Float32Array::from(vec![Some(1.0), None, Some(3.0)])),
                Scalar::Float(1.0),
            ),
            (
                DataType::DOUBLE,
                Arc::new(Float64Array::from(vec![Some(1.0), None, Some(3.0)])),
                Scalar::Double(1.0),
            ),
            (
                DataType::BOOLEAN,
                Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
                Scalar::Boolean(true),
            ),
            (
                DataType::BINARY,
                Arc::new(BinaryArray::from_opt_vec(vec![
                    Some(b"one"),
                    None,
                    Some(b"three"),
                ])),
                Scalar::Binary(b"one".to_vec()),
            ),
            (
                DataType::BINARY,
                Arc::new(LargeBinaryArray::from_opt_vec(vec![
                    Some(b"one"),
                    None,
                    Some(b"three"),
                ])),
                Scalar::Binary(b"one".to_vec()),
            ),
            (
                DataType::BINARY,
                Arc::new(BinaryViewArray::from_iter(vec![
                    Some(b"one".to_vec()),
                    None,
                    Some(b"three".to_vec()),
                ])),
                Scalar::Binary(b"one".to_vec()),
            ),
            // TODO: decimal
            (
                DataType::DATE,
                Arc::new(Date32Array::from(vec![Some(1), None, Some(3)])),
                Scalar::Date(1),
            ),
            (
                DataType::TIMESTAMP,
                Arc::new(
                    TimestampMicrosecondArray::from(vec![Some(1), None, Some(3)])
                        .with_timezone_utc(),
                ),
                Scalar::Timestamp(1),
            ),
            (
                DataType::TIMESTAMP_NTZ,
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(1),
                    None,
                    Some(3),
                ])),
                Scalar::TimestampNtz(1),
            ),
        ];

        // unsupported types due to lack of arrow kernel support
        let unsupported = &[
            ArrowDataType::Utf8View,
            ArrowDataType::Binary,
            ArrowDataType::LargeBinary,
            ArrowDataType::BinaryView,
            ArrowDataType::Boolean,
        ];

        for (data_type, values, list_value) in cases {
            let field = Arc::new(Field::new("item", values.data_type().clone(), true));
            let schema = crate::arrow::datatypes::Schema::new([field.clone()]);
            let batch = RecordBatch::try_new(Arc::new(schema), vec![values.clone()]).unwrap();

            // test "col IN (lit1, lit2, ...)"
            let rhs = Expression::literal(Scalar::Array(ArrayData::new(
                ArrayType::new(data_type.clone(), true),
                [list_value.clone(), Scalar::Null(data_type)],
            )));
            let result = eval_in_list(&batch, &column_expr!("item"), &rhs).unwrap();
            let expected = Arc::new(BooleanArray::from(vec![Some(true), None, None]));
            assert_eq!(result.as_ref(), expected.as_ref());

            // test "lit IN (col)"
            let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 2, 3]));
            let list_field = Arc::new(Field::new("item", ArrowDataType::List(field.clone()), true));
            let schema = crate::arrow::datatypes::Schema::new([list_field.clone()]);
            let array = ListArray::new(field.clone(), offsets, values.clone(), None);
            let batch =
                RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array.clone())]).unwrap();
            let result = eval_in_list(
                &batch,
                &Expression::literal(list_value),
                &column_expr!("item"),
            );
            if unsupported.contains(values.data_type()) {
                assert!(result.is_err());
            } else {
                let result = result.unwrap();
                let expected = Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)]));
                assert_eq!(result.as_ref(), expected.as_ref());
            }
        }
    }

    #[test]
    fn test_eval_in_list_lit_in_lit() {
        let dummy = crate::arrow::datatypes::Schema::new(vec![Field::new(
            "a",
            ArrowDataType::Boolean,
            true,
        )]);
        let dummy_batch =
            RecordBatch::try_new(Arc::new(dummy), vec![Arc::new(BooleanArray::new_null(1))])
                .unwrap();

        let rhs = Expression::literal(Scalar::Array(ArrayData::new(
            ArrayType::new(PrimitiveType::Integer.into(), true),
            [Scalar::Integer(1), Scalar::Null(DataType::INTEGER)],
        )));

        let result =
            eval_in_list(&dummy_batch, &Expression::literal(Scalar::Integer(1)), &rhs).unwrap();
        let expected = Arc::new(BooleanArray::from(vec![Some(true)]));
        assert_eq!(result.as_ref(), expected.as_ref());

        let result = eval_in_list(
            &dummy_batch,
            &Expression::literal(Scalar::Integer(1)),
            &Expression::literal(Scalar::Integer(1)),
        );
        assert!(result.is_err());
    }
}
