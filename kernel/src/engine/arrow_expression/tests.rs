use std::ops::{Add, Div, Mul, Sub};

use crate::arrow::array::{
    create_array, Array, ArrayRef, BooleanArray, GenericStringArray, Int32Array, Int32Builder,
    ListArray, MapArray, MapBuilder, MapFieldNames, StringArray, StringBuilder, StructArray,
};
use crate::arrow::buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use crate::arrow::compute::kernels::cmp::{gt_eq, lt};
use crate::arrow::datatypes::{DataType, Field, Fields, Schema};
use crate::engine::arrow_expression::evaluate_expression::to_json;
use crate::engine::arrow_expression::opaque::{
    ArrowOpaqueExpression as _, ArrowOpaqueExpressionOp, ArrowOpaquePredicate as _,
    ArrowOpaquePredicateOp,
};
use crate::expressions::*;
use crate::kernel_predicates::{
    DirectDataSkippingPredicateEvaluator, DirectPredicateEvaluator,
    IndirectDataSkippingPredicateEvaluator,
};
use crate::schema::{ArrayType, DataType as KernelDataType, MapType, StructField, StructType};
use crate::utils::test_utils::assert_result_error_with_message;
use crate::EvaluationHandlerExtension as _;

use super::*;

use Expression as Expr;
use Predicate as Pred;

#[test]
fn test_array_column() {
    let values = Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8]);
    let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 3, 6, 9]));
    let field = Arc::new(Field::new("item", DataType::Int32, true));
    let arr_field = Arc::new(Field::new("item", DataType::List(field.clone()), true));

    let schema = Schema::new([arr_field.clone()]);

    let array = ListArray::new(field.clone(), offsets, Arc::new(values), None);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array.clone())]).unwrap();

    let not_op = Pred::not(Pred::binary(
        BinaryPredicateOp::In,
        Expr::literal(5),
        column_expr!("item"),
    ));

    let in_op = Pred::binary(
        BinaryPredicateOp::In,
        Expr::literal(5),
        column_expr!("item"),
    );

    let result = evaluate_predicate(&not_op, &batch, false).unwrap();
    let expected_not_in = BooleanArray::from(vec![true, false, true]);
    assert_eq!(result, expected_not_in);

    let result = evaluate_predicate(&in_op, &batch, false).unwrap();
    let expected_in = BooleanArray::from(vec![false, true, false]);
    assert_eq!(result, expected_in);

    // Test inversion as well
    let result = evaluate_predicate(&not_op, &batch, true).unwrap();
    assert_eq!(result, expected_in);

    let result = evaluate_predicate(&in_op, &batch, true).unwrap();
    assert_eq!(result, expected_not_in);
}

#[test]
fn test_bad_right_type_array() {
    let values = Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8]);
    let field = Arc::new(Field::new("item", DataType::Int32, true));
    let schema = Schema::new([field.clone()]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(values.clone())]).unwrap();

    let in_op = Pred::not(Pred::binary(
        BinaryPredicateOp::In,
        Expr::literal(5),
        column_expr!("item"),
    ));

    let in_result = evaluate_predicate(&in_op, &batch, false);

    assert_result_error_with_message(
        in_result,
        "Invalid expression evaluation: Cannot cast to list array: Int32",
    );
}

#[test]
fn test_literal_type_array() {
    let field = Arc::new(Field::new("item", DataType::Int32, true));
    let schema = Schema::new([field.clone()]);
    let batch = RecordBatch::new_empty(Arc::new(schema));

    let not_in_op = Pred::not(Pred::binary(
        BinaryPredicateOp::In,
        Expr::literal(5),
        Scalar::Array(
            ArrayData::try_new(
                ArrayType::new(KernelDataType::INTEGER, false),
                vec![Scalar::Integer(1), Scalar::Integer(2)],
            )
            .unwrap(),
        ),
    ));

    let result = evaluate_predicate(&not_in_op, &batch, false).unwrap();
    let not_in_expected = BooleanArray::from(vec![true]);
    assert_eq!(result, not_in_expected);

    // Test inversion
    let result = evaluate_predicate(&not_in_op, &batch, true).unwrap();
    let in_expected = BooleanArray::from(vec![false]);
    assert_eq!(result, in_expected);
}

#[test]
fn test_literal_complex_type_array() {
    use crate::arrow::array::{Array as _, AsArray as _};
    use crate::arrow::datatypes::Int32Type;

    let array_type = ArrayType::new(KernelDataType::INTEGER, true);
    let array_value = Scalar::Array(
        ArrayData::try_new(
            array_type.clone(),
            vec![
                Scalar::from(1),
                Scalar::from(2),
                Scalar::Null(KernelDataType::INTEGER),
                Scalar::from(3),
            ],
        )
        .unwrap(),
    );
    let map_type = MapType::new(
        KernelDataType::STRING,
        KernelDataType::Array(Box::new(array_type.clone())),
        true,
    );
    let map_value = Scalar::Map(
        MapData::try_new(
            map_type.clone(),
            [
                ("array".to_string(), array_value.clone()),
                (
                    "null_array".to_string(),
                    Scalar::Null(array_type.clone().into()),
                ),
            ],
        )
        .unwrap(),
    );
    let struct_fields = vec![
        StructField::nullable("scalar", KernelDataType::INTEGER),
        StructField::nullable("list", array_type.clone()),
        StructField::nullable("null_list", array_type.clone()),
        StructField::nullable("map", map_type.clone()),
        StructField::nullable("null_map", map_type.clone()),
    ];
    let struct_type = StructType::new_unchecked(struct_fields.clone());
    let struct_value = Scalar::Struct(
        crate::expressions::StructData::try_new(
            struct_fields.clone(),
            vec![
                Scalar::Integer(42),
                array_value,
                Scalar::Null(array_type.clone().into()),
                map_value,
                Scalar::Null(map_type.clone().into()),
            ],
        )
        .unwrap(),
    );
    let nested_array_type = ArrayType::new(struct_type.clone().into(), true);
    let nested_array_value = Scalar::Array(
        ArrayData::try_new(
            nested_array_type.clone(),
            vec![
                struct_value.clone(),
                Scalar::Null(struct_type.clone().into()),
                struct_value.clone(),
            ],
        )
        .unwrap(),
    )
    .to_array(5)
    .unwrap();
    assert_eq!(nested_array_value.len(), 5);

    let struct_values = nested_array_value.as_list::<i32>().values();
    let struct_values = struct_values.as_struct();
    assert_eq!(struct_values.len(), 5 * 3); // five rows, three elements per row

    // each nested array value has three elements, the middle one NULL
    let expected_valid = [true, false, true];
    let expected_valid = (0..5).flat_map(|_| expected_valid.iter().cloned());
    assert!(expected_valid
        .zip(struct_values.nulls().unwrap())
        .all(|(a, b)| a == b));

    let expected_values = [Some(42), None, Some(42)];
    let expected_values = (0..5).flat_map(|_| expected_values.iter().cloned());
    assert!(expected_values
        .zip(struct_values.column(0).as_primitive::<Int32Type>())
        .all(|(a, b)| a == b));
    assert_eq!(struct_values.column(2).null_count(), 15);

    // The leaf value column has 40 elements (not 60) becuase 1/3 of the parent structs are NULL.
    let list_values = struct_values.column(1);
    let list_values = list_values.as_list::<i32>().values();
    assert_eq!(list_values.len(), 40);
    let expected_values = [Some(1), Some(2), None, Some(3)];
    let expected_values = (0..10).flat_map(|_| expected_values.iter().cloned());
    assert!(expected_values
        .zip(list_values.as_primitive::<Int32Type>())
        .all(|(a, b)| a == b));

    let map_values = struct_values.column(3);
    let map_array = map_values.as_map();
    assert_eq!(map_array.keys().len(), 5 * 2 * 2);
    // values len = keys len
    assert_eq!(map_array.values().len(), 5 * 2 * 2);
    // this should be 5 rows * 2 non-null parents * 1 non-null per map * 4 elements
    // NOTE: one of those elements is NULL but primitive arrays don't care about that
    assert_eq!(
        map_array.values().as_list::<i32>().values().len(),
        5 * 2 * 4
    );
    let expected_keys = ["array", "null_array"];
    let expected_values = [Some(1), Some(2), None, Some(3)];
    let expected_keys = (0..10).flat_map(|_| expected_keys.iter().cloned());
    let expected_values = (0..10).flat_map(|_| expected_values.iter().cloned());
    let map_keys = map_array.keys().as_string::<i32>();
    assert!(expected_keys.zip(map_keys).all(|(a, b)| a == b.unwrap()));
    let map_values = map_array
        .values()
        .as_list::<i32>()
        .values()
        .as_primitive::<Int32Type>();
    assert!(expected_values.zip(map_values).all(|(a, b)| a == b));
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

    let in_op = Pred::not(Pred::binary(
        BinaryPredicateOp::In,
        column_expr!("item"),
        column_expr!("item"),
    ));

    let in_result = evaluate_predicate(&in_op, &batch, false);

    assert_result_error_with_message(in_result, "Invalid expression evaluation: Invalid right value for (NOT) IN comparison, left is: Column(item) right is: Column(item)");
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

    let str_not_op = Pred::not(Pred::binary(
        BinaryPredicateOp::In,
        Expr::literal("bye"),
        column_expr!("item"),
    ));

    let str_in_op = Pred::binary(
        BinaryPredicateOp::In,
        Expr::literal("hi"),
        column_expr!("item"),
    );

    let result = evaluate_predicate(&str_in_op, &batch, false).unwrap();
    let in_expected = BooleanArray::from(vec![true, true, true]);
    assert_eq!(result, in_expected);

    let result = evaluate_predicate(&str_not_op, &batch, false).unwrap();
    let not_in_expected = BooleanArray::from(vec![false, false, false]);
    assert_eq!(result, not_in_expected);

    // Test inversion
    let result = evaluate_predicate(&str_in_op, &batch, true).unwrap();
    assert_eq!(result, not_in_expected);

    let result = evaluate_predicate(&str_not_op, &batch, true).unwrap();
    assert_eq!(result, in_expected);
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

    let expression = column.clone().add(Expr::literal(1));
    let results = evaluate_expression(&expression, &batch, None).unwrap();
    let expected = Arc::new(Int32Array::from(vec![2, 3, 4]));
    assert_eq!(results.as_ref(), expected.as_ref());

    let expression = column.clone().sub(Expr::literal(1));
    let results = evaluate_expression(&expression, &batch, None).unwrap();
    let expected = Arc::new(Int32Array::from(vec![0, 1, 2]));
    assert_eq!(results.as_ref(), expected.as_ref());

    let expression = column.clone().mul(Expr::literal(2));
    let results = evaluate_expression(&expression, &batch, None).unwrap();
    let expected = Arc::new(Int32Array::from(vec![2, 4, 6]));
    assert_eq!(results.as_ref(), expected.as_ref());

    // TODO handle type casting
    let expression = column.div(Expr::literal(1));
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

    let predicate_lt = column.clone().lt(Expr::literal(2));
    let results = evaluate_predicate(&predicate_lt, &batch, false).unwrap();
    let expected_lt = BooleanArray::from(vec![true, false, false]);
    assert_eq!(results, expected_lt);

    let predicate_le = column.clone().le(Expr::literal(2));
    let results = evaluate_predicate(&predicate_le, &batch, false).unwrap();
    let expected_le = BooleanArray::from(vec![true, true, false]);
    assert_eq!(results, expected_le);

    let predicate_gt = column.clone().gt(Expr::literal(2));
    let results = evaluate_predicate(&predicate_gt, &batch, false).unwrap();
    let expected_gt = BooleanArray::from(vec![false, false, true]);
    assert_eq!(results, expected_gt);

    let predicate_ge = column.clone().ge(Expr::literal(2));
    let results = evaluate_predicate(&predicate_ge, &batch, false).unwrap();
    let expected_ge = BooleanArray::from(vec![false, true, true]);
    assert_eq!(results, expected_ge);

    let predicate_eq = column.clone().eq(Expr::literal(2));
    let results = evaluate_predicate(&predicate_eq, &batch, false).unwrap();
    let expected_eq = BooleanArray::from(vec![false, true, false]);
    assert_eq!(results, expected_eq);

    let predicate_ne = column.clone().ne(Expr::literal(2));
    let results = evaluate_predicate(&predicate_ne, &batch, false).unwrap();
    let expected_ne = BooleanArray::from(vec![true, false, true]);
    assert_eq!(results, expected_ne);

    // Test inversion
    let results = evaluate_predicate(&predicate_lt, &batch, true).unwrap();
    assert_eq!(results, expected_ge);

    let results = evaluate_predicate(&predicate_le, &batch, true).unwrap();
    assert_eq!(results, expected_gt);

    let results = evaluate_predicate(&predicate_gt, &batch, true).unwrap();
    assert_eq!(results, expected_le);

    let results = evaluate_predicate(&predicate_ge, &batch, true).unwrap();
    assert_eq!(results, expected_lt);

    let results = evaluate_predicate(&predicate_eq, &batch, true).unwrap();
    assert_eq!(results, expected_ne);

    let results = evaluate_predicate(&predicate_ne, &batch, true).unwrap();
    assert_eq!(results, expected_eq);
}

#[test]
fn test_logical() {
    let t = Some(true);
    let f = Some(false);
    let n = None;

    let schema = Schema::new(vec![
        Field::new("a", DataType::Boolean, false),
        Field::new("b", DataType::Boolean, true),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(BooleanArray::from(vec![t, t, f, f, t, f])),
            Arc::new(BooleanArray::from(vec![t, f, t, f, n, n])),
        ],
    )
    .unwrap();
    let column_a = column_pred!("a");
    let column_b = column_pred!("b");

    let pred_and_col = Pred::and(column_a.clone(), column_b.clone());
    let results = evaluate_predicate(&pred_and_col, &batch, false).unwrap();
    let expected = BooleanArray::from(vec![t, f, f, f, n, f]);
    assert_eq!(results, expected);

    let pred_and_lit = Pred::and(column_a.clone(), Pred::literal(true));
    let results = evaluate_predicate(&pred_and_lit, &batch, false).unwrap();
    let expected = BooleanArray::from(vec![t, t, f, f, t, f]);
    assert_eq!(results, expected);

    let pred_or_col = Pred::or(column_a.clone(), column_b);
    let results = evaluate_predicate(&pred_or_col, &batch, false).unwrap();
    let expected = BooleanArray::from(vec![t, t, t, f, t, n]);
    assert_eq!(results, expected);

    let pred_or_lit = Pred::or(column_a.clone(), Pred::literal(false));
    let results = evaluate_predicate(&pred_or_lit, &batch, false).unwrap();
    let expected = BooleanArray::from(vec![t, t, f, f, t, f]);
    assert_eq!(results, expected);

    // Test inversion
    let results = evaluate_predicate(&pred_and_col, &batch, true).unwrap();
    let expected = BooleanArray::from(vec![f, t, t, t, n, t]);
    assert_eq!(results, expected);

    let results = evaluate_predicate(&pred_and_lit, &batch, true).unwrap();
    let expected = BooleanArray::from(vec![f, f, t, t, f, t]);
    assert_eq!(results, expected);

    let results = evaluate_predicate(&pred_or_col, &batch, true).unwrap();
    let expected = BooleanArray::from(vec![f, f, f, t, f, n]);
    assert_eq!(results, expected);

    let results = evaluate_predicate(&pred_or_lit, &batch, true).unwrap();
    let expected = BooleanArray::from(vec![f, f, t, t, f, t]);
    assert_eq!(results, expected);
}

#[derive(Debug, PartialEq)]
struct OpaqueLessThanOp;

impl OpaqueLessThanOp {
    fn name(&self) -> &str {
        "less_than"
    }

    fn eval_pred(
        &self,
        args: &[Expression],
        batch: &RecordBatch,
        inverted: bool,
    ) -> DeltaResult<BooleanArray> {
        let op_fn = match inverted {
            true => gt_eq,
            false => lt,
        };

        let [left, right] = args else {
            panic!("Invalid arg count: {}", args.len());
        };

        let eval = |arg| evaluate_expression(arg, batch, Some(&KernelDataType::BOOLEAN));
        Ok(op_fn(&eval(left)?, &eval(right)?)?)
    }
}

impl ArrowOpaqueExpressionOp for OpaqueLessThanOp {
    fn name(&self) -> &str {
        self.name()
    }

    fn eval_expr_scalar(
        &self,
        _eval_expr: &ScalarExpressionEvaluator<'_>,
        _exprs: &[Expression],
    ) -> DeltaResult<Scalar> {
        unimplemented!() // OpaqueExpressionOp is already tested
    }

    fn eval_expr(
        &self,
        args: &[Expression],
        batch: &RecordBatch,
        result_type: Option<&KernelDataType>,
    ) -> DeltaResult<ArrayRef> {
        assert!(matches!(result_type, None | Some(&KernelDataType::BOOLEAN)));
        let result = self.eval_pred(args, batch, false)?;
        Ok(Arc::new(result))
    }
}

impl ArrowOpaquePredicateOp for OpaqueLessThanOp {
    fn name(&self) -> &str {
        self.name()
    }

    fn eval_pred(
        &self,
        args: &[Expression],
        batch: &RecordBatch,
        inverted: bool,
    ) -> DeltaResult<BooleanArray> {
        self.eval_pred(args, batch, inverted)
    }

    fn eval_pred_scalar(
        &self,
        _eval_expr: &ScalarExpressionEvaluator<'_>,
        _eval_pred: &DirectPredicateEvaluator<'_>,
        _exprs: &[Expression],
        _inverted: bool,
    ) -> DeltaResult<Option<bool>> {
        unimplemented!() // OpaquePredicateOp is already tested
    }

    fn eval_as_data_skipping_predicate(
        &self,
        _predicate_evaluator: &DirectDataSkippingPredicateEvaluator<'_>,
        _exprs: &[Expr],
        _inverted: bool,
    ) -> Option<bool> {
        unimplemented!() // OpaquePredicateOp is already tested
    }

    fn as_data_skipping_predicate(
        &self,
        _predicate_evaluator: &IndirectDataSkippingPredicateEvaluator<'_>,
        _exprs: &[Expr],
        _inverted: bool,
    ) -> Option<Pred> {
        unimplemented!() // OpaquePredicateOp is already tested
    }
}

#[test]
fn test_opaque() {
    let expr = Expr::arrow_opaque(OpaqueLessThanOp, [column_expr!("x"), Expr::literal(10)]);
    let pred = Pred::arrow_opaque(OpaqueLessThanOp, [column_expr!("x"), Expr::literal(10)]);

    assert_eq!(
        format!("{expr:?}"),
        "Opaque(OpaqueExpression { op: ArrowOpaqueExpressionOpAdaptor(OpaqueLessThanOp), exprs: [Column(ColumnName { path: [\"x\"] }), Literal(Integer(10))] })"
    );
    assert_eq!(
        format!("{pred:?}"),
        "Opaque(OpaquePredicate { op: ArrowOpaquePredicateOpAdaptor(OpaqueLessThanOp), exprs: [Column(ColumnName { path: [\"x\"] }), Literal(Integer(10))] })"
    );

    assert_eq!(expr, expr);
    assert_eq!(pred, pred);

    let data = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1, 10, 100]))],
    )
    .unwrap();

    let lt_result = evaluate_predicate(&pred, &data, false).unwrap();
    let lt_expected = BooleanArray::from(vec![true, false, false]);
    assert_eq!(lt_result, lt_expected);

    let not_lt_result = evaluate_predicate(&pred, &data, true).unwrap();
    let not_lt_expected = BooleanArray::from(vec![false, true, true]);
    assert_eq!(not_lt_result, not_lt_expected);

    let lt_result = evaluate_expression(&expr, &data, Some(&KernelDataType::BOOLEAN)).unwrap();
    assert_eq!(lt_result.as_ref(), &lt_expected);
}

#[test]
fn test_null_row() {
    // note that we _allow_ nested nulls, since the top-level struct can be NULL
    let schema = Arc::new(StructType::new_unchecked(vec![
        StructField::nullable(
            "x",
            StructType::new_unchecked([
                StructField::nullable("a", KernelDataType::INTEGER),
                StructField::not_null("b", KernelDataType::STRING),
            ]),
        ),
        StructField::nullable("c", KernelDataType::STRING),
    ]));
    let handler = ArrowEvaluationHandler;
    let result = handler.null_row(schema.clone()).unwrap();
    let expected = RecordBatch::try_new(
        Arc::new(schema.as_ref().try_into_arrow().unwrap()),
        vec![
            Arc::new(StructArray::new_null(
                [
                    Arc::new(Field::new("a", DataType::Int32, true)),
                    Arc::new(Field::new("b", DataType::Utf8, false)),
                ]
                .into(),
                1,
            )),
            create_array!(Utf8, [None::<String>]),
        ],
    )
    .unwrap();
    let result: RecordBatch = result
        .into_any()
        .downcast::<ArrowEngineData>()
        .unwrap()
        .into();
    assert_eq!(result, expected);
}

#[test]
fn test_null_row_err() {
    let not_null_schema = Arc::new(StructType::new_unchecked(vec![StructField::not_null(
        "a",
        KernelDataType::STRING,
    )]));
    let handler = ArrowEvaluationHandler;
    assert_result_error_with_message(
        handler.null_row(not_null_schema),
        "Invalid argument error: Column 'a' is declared as non-nullable but contains null values",
    );
}

// helper to take values/schema to pass to `create_one` and assert the result = expected
fn assert_create_one(values: &[Scalar], schema: SchemaRef, expected: RecordBatch) {
    let handler = ArrowEvaluationHandler;
    let actual = handler.create_one(schema, values).unwrap();
    let actual_rb: RecordBatch = actual
        .into_any()
        .downcast::<ArrowEngineData>()
        .unwrap()
        .into();
    assert_eq!(actual_rb, expected);
}

#[test]
fn test_create_one() {
    let values: &[Scalar] = &[
        1.into(),
        "B".into(),
        3.into(),
        Scalar::Null(KernelDataType::INTEGER),
    ];
    let schema = Arc::new(StructType::new_unchecked([
        StructField::nullable("a", KernelDataType::INTEGER),
        StructField::nullable("b", KernelDataType::STRING),
        StructField::not_null("c", KernelDataType::INTEGER),
        StructField::nullable("d", KernelDataType::INTEGER),
    ]));

    let expected_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Utf8, true),
        Field::new("c", DataType::Int32, false),
        Field::new("d", DataType::Int32, true),
    ]));
    let expected = RecordBatch::try_new(
        expected_schema,
        vec![
            create_array!(Int32, [1]),
            create_array!(Utf8, ["B"]),
            create_array!(Int32, [3]),
            create_array!(Int32, [None]),
        ],
    )
    .unwrap();
    assert_create_one(values, schema, expected);
}

#[test]
fn test_create_one_nested() {
    let values: &[Scalar] = &[1.into(), 2.into()];
    let schema = Arc::new(StructType::new_unchecked([StructField::not_null(
        "a",
        KernelDataType::struct_type_unchecked([
            StructField::nullable("b", KernelDataType::INTEGER),
            StructField::not_null("c", KernelDataType::INTEGER),
        ]),
    )]));
    let expected_schema = Arc::new(Schema::new(vec![Field::new(
        "a",
        DataType::Struct(
            vec![
                Field::new("b", DataType::Int32, true),
                Field::new("c", DataType::Int32, false),
            ]
            .into(),
        ),
        false,
    )]));
    let expected = RecordBatch::try_new(
        expected_schema,
        vec![Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("b", DataType::Int32, true)),
                create_array!(Int32, [1]) as ArrayRef,
            ),
            (
                Arc::new(Field::new("c", DataType::Int32, false)),
                create_array!(Int32, [2]) as ArrayRef,
            ),
        ]))],
    )
    .unwrap();
    assert_create_one(values, schema, expected);
}

#[test]
fn test_create_one_nested_null() {
    let values: &[Scalar] = &[Scalar::Null(KernelDataType::INTEGER), 1.into()];
    let schema = Arc::new(StructType::new_unchecked([StructField::not_null(
        "a",
        KernelDataType::struct_type_unchecked([
            StructField::nullable("b", KernelDataType::INTEGER),
            StructField::not_null("c", KernelDataType::INTEGER),
        ]),
    )]));
    let expected_schema = Arc::new(Schema::new(vec![Field::new(
        "a",
        DataType::Struct(
            vec![
                Field::new("b", DataType::Int32, true),
                Field::new("c", DataType::Int32, false),
            ]
            .into(),
        ),
        false,
    )]));
    let expected = RecordBatch::try_new(
        expected_schema,
        vec![Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("b", DataType::Int32, true)),
                create_array!(Int32, [None]) as ArrayRef,
            ),
            (
                Arc::new(Field::new("c", DataType::Int32, false)),
                create_array!(Int32, [1]) as ArrayRef,
            ),
        ]))],
    )
    .unwrap();
    assert_create_one(values, schema, expected);
}

#[test]
fn test_create_one_mismatching_scalar_types() {
    // Scalar is a LONG but schema specifies INTEGER
    let values: &[Scalar] = &[Scalar::Long(10)];
    let schema = Arc::new(StructType::new_unchecked([StructField::not_null(
        "version",
        KernelDataType::INTEGER,
    )]));
    let handler = ArrowEvaluationHandler;
    assert_result_error_with_message(
        handler.create_one(schema, values),
        "Schema error: Mismatched scalar type while creating Expression: expected Integer, got Long",
    );
}

#[test]
fn test_create_one_not_null_struct() {
    let values: &[Scalar] = &[
        Scalar::Null(KernelDataType::INTEGER),
        Scalar::Null(KernelDataType::INTEGER),
    ];
    let schema = Arc::new(StructType::new_unchecked([StructField::not_null(
        "a",
        KernelDataType::struct_type_unchecked([
            StructField::not_null("b", KernelDataType::INTEGER),
            StructField::nullable("c", KernelDataType::INTEGER),
        ]),
    )]));
    let handler = ArrowEvaluationHandler;
    assert_result_error_with_message(
        handler.create_one(schema, values),
        "Invalid struct data: Top-level nulls in struct are not supported",
    );
}

#[test]
fn test_create_one_top_level_null() {
    let values = &[Scalar::Null(KernelDataType::INTEGER)];
    let handler = ArrowEvaluationHandler;

    let schema = Arc::new(StructType::new_unchecked([StructField::not_null(
        "col_1",
        KernelDataType::INTEGER,
    )]));
    assert!(matches!(
        handler.create_one(schema, values),
        Err(Error::InvalidStructData(_))
    ));
}

#[test]
fn test_scalar_map() -> DeltaResult<()> {
    // making an 2-row array each with a map with 2 pairs.
    // result: { key1: 1, key2: null }, { key1: 1, key2: null }
    let map_type = MapType::new(KernelDataType::STRING, KernelDataType::INTEGER, true);
    let map_data = MapData::try_new(
        map_type,
        [("key1".to_string(), 1.into()), ("key2".to_string(), None)],
    )?;
    let scalar_map = Scalar::Map(map_data);
    let arrow_array = scalar_map.to_array(2)?;
    let map_array = arrow_array.as_any().downcast_ref::<MapArray>().unwrap();

    let key_builder = StringBuilder::new();
    let val_builder = Int32Builder::new();
    let names = MapFieldNames {
        entry: "key_values".to_string(),
        key: "keys".to_string(),
        value: "values".to_string(),
    };
    let mut builder = MapBuilder::new(Some(names), key_builder, val_builder);
    builder.keys().append_value("key1");
    builder.values().append_value(1);
    builder.keys().append_value("key2");
    builder.values().append_null();
    builder.append(true).unwrap();
    builder.keys().append_value("key1");
    builder.values().append_value(1);
    builder.keys().append_value("key2");
    builder.values().append_null();
    builder.append(true).unwrap();
    let expected = builder.finish();

    assert_eq!(map_array, &expected);
    Ok(())
}

#[test]
fn test_null_scalar_map() -> DeltaResult<()> {
    let map_type = MapType::new(KernelDataType::STRING, KernelDataType::STRING, false);
    let null_scalar_map = Scalar::Null(KernelDataType::Map(Box::new(map_type)));
    let arrow_array = null_scalar_map.to_array(1)?;
    let map_array = arrow_array.as_any().downcast_ref::<MapArray>().unwrap();

    assert_eq!(map_array.len(), 1);
    assert_eq!(map_array.null_count(), 1);
    assert!(map_array.is_null(0));

    Ok(())
}

#[test]
fn test_apply_schema_column_count_mismatch() {
    use super::apply_schema::apply_schema;
    use crate::schema::StructType;

    // Create a struct array with 3 columns
    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("a", DataType::Int32, false)),
            create_array!(Int32, [1]) as ArrayRef,
        ),
        (
            Arc::new(Field::new("b", DataType::Int32, false)),
            create_array!(Int32, [2]) as ArrayRef,
        ),
        (
            Arc::new(Field::new("c", DataType::Int32, false)),
            create_array!(Int32, [3]) as ArrayRef,
        ),
    ]);

    // Create a schema with only 2 fields (mismatch)
    let schema = KernelDataType::Struct(Box::new(StructType::new_unchecked([
        StructField::not_null("a", KernelDataType::INTEGER),
        StructField::not_null("b", KernelDataType::INTEGER),
    ])));

    let result = apply_schema(&struct_array, &schema);

    assert_result_error_with_message(
        result,
        "Passed struct had 3 columns, but transformed column has 2",
    );
}

#[test]
fn test_to_json_with_struct_array() {
    // Create a test struct array
    let boolean_field = Arc::new(Field::new("bool_field", ArrowDataType::Boolean, true));
    let int_field = Arc::new(Field::new("int_field", ArrowDataType::Int32, true));
    let string_field = Arc::new(Field::new("string_field", ArrowDataType::Utf8, true));

    let boolean_array = Arc::new(BooleanArray::from(vec![
        Some(true),
        Some(false),
        None,
        None,
        None,
    ]));
    let int_array = Arc::new(Int32Array::from(vec![Some(42), None, Some(84), None, None]));
    let string_array = Arc::new(StringArray::from(vec![
        Some("hello"),
        Some("world"),
        Some("test"),
        None,
        None,
    ]));

    let struct_array = StructArray::new(
        vec![boolean_field, int_field, string_field].into(),
        vec![boolean_array, int_array, string_array],
        Some(NullBuffer::new(BooleanBuffer::from(vec![
            true, true, true, true, false,
        ]))),
    );

    // Test the to_json function
    let result = to_json(&struct_array).unwrap();
    let json_array = result.as_any().downcast_ref::<StringArray>().unwrap();

    assert_eq!(json_array.len(), 5);
    assert_eq!(
        json_array.value(0),
        r#"{"bool_field":true,"int_field":42,"string_field":"hello"}"#
    );
    assert_eq!(
        json_array.value(1),
        r#"{"bool_field":false,"string_field":"world"}"#
    );
    assert_eq!(
        json_array.value(2),
        r#"{"int_field":84,"string_field":"test"}"#
    );
    // All fields of the struct row are null
    assert_eq!(json_array.value(3), r#"{}"#);
    // The struct row itself is null
    assert!(json_array.is_null(4));
}

#[test]
fn test_to_json_with_null_struct() {
    // Create a test struct array with a NullBuffer
    let int_field = Arc::new(Field::new("int_field", ArrowDataType::Int32, true));
    let int_array = Arc::new(Int32Array::from(vec![Some(42), Some(24)]));

    let struct_array = StructArray::new(
        vec![int_field].into(),
        vec![int_array],
        Some(crate::arrow::buffer::NullBuffer::new(
            crate::arrow::buffer::BooleanBuffer::from(vec![true, false]),
        )),
    );

    // Test the to_json function
    let result = to_json(&struct_array).unwrap();
    let json_array = result.as_any().downcast_ref::<StringArray>().unwrap();

    assert_eq!(json_array.len(), 2);
    assert!(!json_array.is_null(0));
    assert!(json_array.is_null(1));
    assert_eq!(json_array.value(0), r#"{"int_field":42}"#);
}

#[test]
fn test_to_json_with_non_struct_array() {
    // Test that to_json fails when input is not a StructArray
    let int_array = Int32Array::from(vec![1, 2, 3]);
    let result = to_json(&int_array);
    assert_result_error_with_message(result, "TO_JSON can only be applied to struct arrays");

    let string_array = StringArray::from(vec!["hello", "world"]);
    let result = to_json(&string_array);
    assert_result_error_with_message(result, "TO_JSON can only be applied to struct arrays");

    let boolean_array = BooleanArray::from(vec![true, false]);
    let result = to_json(&boolean_array);
    assert_result_error_with_message(result, "TO_JSON can only be applied to struct arrays");
}

#[test]
fn test_to_json_with_empty_struct_array() {
    // Test to_json with an empty struct array
    let int_field = Arc::new(Field::new("int_field", ArrowDataType::Int32, true));
    let int_array = Arc::new(Int32Array::from(Vec::<Option<i32>>::new()));

    let struct_array = StructArray::new(vec![int_field].into(), vec![int_array], None);

    let result = to_json(&struct_array).unwrap();
    let json_array = result.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(json_array.len(), 0);
}

#[test]
fn test_to_json_with_nested_struct() {
    // Test to_json with nested struct fields
    let inner_int_field = Arc::new(Field::new("inner_int", ArrowDataType::Int32, true));
    let inner_string_field = Arc::new(Field::new("inner_string", ArrowDataType::Utf8, true));

    let inner_int_array = Arc::new(Int32Array::from(vec![Some(10), None]));
    let inner_string_array = Arc::new(StringArray::from(vec![Some("nested"), Some("value")]));

    let inner_struct_array = Arc::new(StructArray::new(
        vec![inner_int_field, inner_string_field].into(),
        vec![inner_int_array, inner_string_array],
        None,
    ));

    let outer_field = Arc::new(Field::new("outer_int", ArrowDataType::Int32, true));
    let nested_field = Arc::new(Field::new(
        "nested_struct",
        ArrowDataType::Struct(
            vec![
                Field::new("inner_int", ArrowDataType::Int32, true),
                Field::new("inner_string", ArrowDataType::Utf8, true),
            ]
            .into(),
        ),
        true,
    ));

    let outer_array = Arc::new(Int32Array::from(vec![Some(100), Some(200)]));

    let struct_array = StructArray::new(
        vec![outer_field, nested_field].into(),
        vec![outer_array, inner_struct_array],
        None,
    );

    let result = to_json(&struct_array).unwrap();
    let json_array = result.as_any().downcast_ref::<StringArray>().unwrap();

    assert_eq!(json_array.len(), 2);
    assert_eq!(
        json_array.value(0),
        r#"{"outer_int":100,"nested_struct":{"inner_int":10,"inner_string":"nested"}}"#
    );
    assert_eq!(
        json_array.value(1),
        r#"{"outer_int":200,"nested_struct":{"inner_string":"value"}}"#
    );
}
