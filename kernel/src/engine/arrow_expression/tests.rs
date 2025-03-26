use std::ops::{Add, Div, Mul, Sub};

use crate::arrow::array::{
    ArrayRef, BooleanArray, GenericStringArray, Int32Array, ListArray, StructArray,
};
use crate::arrow::buffer::{OffsetBuffer, ScalarBuffer};
use crate::arrow::datatypes::{DataType, Field, Fields, Schema};

use super::*;
use crate::expressions::*;
use crate::schema::ArrayType;
use crate::DataType as DeltaDataTypes;

#[test]
fn test_array_column() {
    let values = Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8]);
    let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 3, 6, 9]));
    let field = Arc::new(Field::new("item", DataType::Int32, true));
    let arr_field = Arc::new(Field::new("item", DataType::List(field.clone()), true));

    let schema = Schema::new([arr_field.clone()]);

    let array = ListArray::new(field.clone(), offsets, Arc::new(values), None);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array.clone())]).unwrap();

    let not_op = Expression::binary(BinaryOperator::NotIn, 5, column_expr!("item"));

    let in_op = Expression::binary(BinaryOperator::In, 5, column_expr!("item"));

    let result = evaluate_expression(&not_op, &batch, None).unwrap();
    let expected = BooleanArray::from(vec![true, false, true]);
    assert_eq!(result.as_ref(), &expected);

    let in_result = evaluate_expression(&in_op, &batch, None).unwrap();
    let in_expected = BooleanArray::from(vec![false, true, false]);
    assert_eq!(in_result.as_ref(), &in_expected);
}

#[test]
fn test_bad_right_type_array() {
    let values = Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8]);
    let field = Arc::new(Field::new("item", DataType::Int32, true));
    let schema = Schema::new([field.clone()]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(values.clone())]).unwrap();

    let in_op = Expression::binary(BinaryOperator::NotIn, 5, column_expr!("item"));

    let in_result = evaluate_expression(&in_op, &batch, None);

    assert!(in_result.is_err());
    assert_eq!(
        in_result.unwrap_err().to_string(),
        "Invalid expression evaluation: Cannot cast to list array: Int32"
    );
}

#[test]
fn test_literal_type_array() {
    let field = Arc::new(Field::new("item", DataType::Int32, true));
    let schema = Schema::new([field.clone()]);
    let batch = RecordBatch::new_empty(Arc::new(schema));

    let in_op = Expression::binary(
        BinaryOperator::NotIn,
        5,
        Scalar::Array(ArrayData::new(
            ArrayType::new(DeltaDataTypes::INTEGER, false),
            vec![Scalar::Integer(1), Scalar::Integer(2)],
        )),
    );

    let in_result = evaluate_expression(&in_op, &batch, None).unwrap();
    let in_expected = BooleanArray::from(vec![true]);
    assert_eq!(in_result.as_ref(), &in_expected);
}

#[test]
fn test_invalid_array_sides() {
    let values = Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8]);
    let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 3, 6, 9]));
    let field = Arc::new(Field::new("item", DataType::Int32, true));
    let arr_field = Arc::new(Field::new("item", DataType::List(field.clone()), true));

    let schema = Schema::new([arr_field.clone()]);

    let array = ListArray::new(field.clone(), offsets, Arc::new(values), None);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array.clone())]).unwrap();

    let in_op = Expression::binary(
        BinaryOperator::NotIn,
        column_expr!("item"),
        column_expr!("item"),
    );

    let in_result = evaluate_expression(&in_op, &batch, None);

    assert!(in_result.is_err());
    assert_eq!(
            in_result.unwrap_err().to_string(),
            "Invalid expression evaluation: Invalid right value for (NOT) IN comparison, left is: Column(item) right is: Column(item)".to_string()
        )
}

#[test]
fn test_str_arrays() {
    let values = GenericStringArray::<i32>::from(vec![
        "hi", "bye", "hi", "hi", "bye", "bye", "hi", "bye", "hi",
    ]);
    let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 3, 6, 9]));
    let field = Arc::new(Field::new("item", DataType::Utf8, true));
    let arr_field = Arc::new(Field::new("item", DataType::List(field.clone()), true));
    let schema = Schema::new([arr_field.clone()]);
    let array = ListArray::new(field.clone(), offsets, Arc::new(values), None);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array.clone())]).unwrap();

    let str_not_op = Expression::binary(BinaryOperator::NotIn, "bye", column_expr!("item"));

    let str_in_op = Expression::binary(BinaryOperator::In, "hi", column_expr!("item"));

    let result = evaluate_expression(&str_in_op, &batch, None).unwrap();
    let expected = BooleanArray::from(vec![true, true, true]);
    assert_eq!(result.as_ref(), &expected);

    let in_result = evaluate_expression(&str_not_op, &batch, None).unwrap();
    let in_expected = BooleanArray::from(vec![false, false, false]);
    assert_eq!(in_result.as_ref(), &in_expected);
}

#[test]
fn test_extract_column() {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
    let values = Int32Array::from(vec![1, 2, 3]);
    let batch =
        RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(values.clone())]).unwrap();
    let column = column_expr!("a");

    let results = evaluate_expression(&column, &batch, None).unwrap();
    assert_eq!(results.as_ref(), &values);

    let schema = Schema::new(vec![Field::new(
        "b",
        DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int32, false)])),
        false,
    )]);

    let struct_values: ArrayRef = Arc::new(values.clone());
    let struct_array = StructArray::from(vec![(
        Arc::new(Field::new("a", DataType::Int32, false)),
        struct_values,
    )]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(struct_array.clone())],
    )
    .unwrap();
    let column = column_expr!("b.a");
    let results = evaluate_expression(&column, &batch, None).unwrap();
    assert_eq!(results.as_ref(), &values);
}

#[test]
fn test_binary_op_scalar() {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
    let values = Int32Array::from(vec![1, 2, 3]);
    let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(values)]).unwrap();
    let column = column_expr!("a");

    let expression = column.clone().add(1);
    let results = evaluate_expression(&expression, &batch, None).unwrap();
    let expected = Arc::new(Int32Array::from(vec![2, 3, 4]));
    assert_eq!(results.as_ref(), expected.as_ref());

    let expression = column.clone().sub(1);
    let results = evaluate_expression(&expression, &batch, None).unwrap();
    let expected = Arc::new(Int32Array::from(vec![0, 1, 2]));
    assert_eq!(results.as_ref(), expected.as_ref());

    let expression = column.clone().mul(2);
    let results = evaluate_expression(&expression, &batch, None).unwrap();
    let expected = Arc::new(Int32Array::from(vec![2, 4, 6]));
    assert_eq!(results.as_ref(), expected.as_ref());

    // TODO handle type casting
    let expression = column.div(1);
    let results = evaluate_expression(&expression, &batch, None).unwrap();
    let expected = Arc::new(Int32Array::from(vec![1, 2, 3]));
    assert_eq!(results.as_ref(), expected.as_ref())
}

#[test]
fn test_binary_op() {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]);
    let values = Int32Array::from(vec![1, 2, 3]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(values.clone()), Arc::new(values)],
    )
    .unwrap();
    let column_a = column_expr!("a");
    let column_b = column_expr!("b");

    let expression = column_a.clone().add(column_b.clone());
    let results = evaluate_expression(&expression, &batch, None).unwrap();
    let expected = Arc::new(Int32Array::from(vec![2, 4, 6]));
    assert_eq!(results.as_ref(), expected.as_ref());

    let expression = column_a.clone().sub(column_b.clone());
    let results = evaluate_expression(&expression, &batch, None).unwrap();
    let expected = Arc::new(Int32Array::from(vec![0, 0, 0]));
    assert_eq!(results.as_ref(), expected.as_ref());

    let expression = column_a.clone().mul(column_b);
    let results = evaluate_expression(&expression, &batch, None).unwrap();
    let expected = Arc::new(Int32Array::from(vec![1, 4, 9]));
    assert_eq!(results.as_ref(), expected.as_ref());
}

#[test]
fn test_binary_cmp() {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
    let values = Int32Array::from(vec![1, 2, 3]);
    let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(values)]).unwrap();
    let column = column_expr!("a");

    let expression = column.clone().lt(2);
    let results = evaluate_expression(&expression, &batch, None).unwrap();
    let expected = Arc::new(BooleanArray::from(vec![true, false, false]));
    assert_eq!(results.as_ref(), expected.as_ref());

    let expression = column.clone().lt_eq(2);
    let results = evaluate_expression(&expression, &batch, None).unwrap();
    let expected = Arc::new(BooleanArray::from(vec![true, true, false]));
    assert_eq!(results.as_ref(), expected.as_ref());

    let expression = column.clone().gt(2);
    let results = evaluate_expression(&expression, &batch, None).unwrap();
    let expected = Arc::new(BooleanArray::from(vec![false, false, true]));
    assert_eq!(results.as_ref(), expected.as_ref());

    let expression = column.clone().gt_eq(2);
    let results = evaluate_expression(&expression, &batch, None).unwrap();
    let expected = Arc::new(BooleanArray::from(vec![false, true, true]));
    assert_eq!(results.as_ref(), expected.as_ref());

    let expression = column.clone().eq(2);
    let results = evaluate_expression(&expression, &batch, None).unwrap();
    let expected = Arc::new(BooleanArray::from(vec![false, true, false]));
    assert_eq!(results.as_ref(), expected.as_ref());

    let expression = column.clone().ne(2);
    let results = evaluate_expression(&expression, &batch, None).unwrap();
    let expected = Arc::new(BooleanArray::from(vec![true, false, true]));
    assert_eq!(results.as_ref(), expected.as_ref());
}

#[test]
fn test_logical() {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Boolean, false),
        Field::new("b", DataType::Boolean, false),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(BooleanArray::from(vec![true, false])),
            Arc::new(BooleanArray::from(vec![false, true])),
        ],
    )
    .unwrap();
    let column_a = column_expr!("a");
    let column_b = column_expr!("b");

    let expression = column_a.clone().and(column_b.clone());
    let results =
        evaluate_expression(&expression, &batch, Some(&crate::schema::DataType::BOOLEAN)).unwrap();
    let expected = Arc::new(BooleanArray::from(vec![false, false]));
    assert_eq!(results.as_ref(), expected.as_ref());

    let expression = column_a.clone().and(true);
    let results =
        evaluate_expression(&expression, &batch, Some(&crate::schema::DataType::BOOLEAN)).unwrap();
    let expected = Arc::new(BooleanArray::from(vec![true, false]));
    assert_eq!(results.as_ref(), expected.as_ref());

    let expression = column_a.clone().or(column_b);
    let results =
        evaluate_expression(&expression, &batch, Some(&crate::schema::DataType::BOOLEAN)).unwrap();
    let expected = Arc::new(BooleanArray::from(vec![true, true]));
    assert_eq!(results.as_ref(), expected.as_ref());

    let expression = column_a.clone().or(false);
    let results =
        evaluate_expression(&expression, &batch, Some(&crate::schema::DataType::BOOLEAN)).unwrap();
    let expected = Arc::new(BooleanArray::from(vec![true, false]));
    assert_eq!(results.as_ref(), expected.as_ref());
}
