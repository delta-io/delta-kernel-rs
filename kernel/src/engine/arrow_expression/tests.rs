use std::ops::{Add, Div, Mul, Sub};

use crate::arrow::array::{
    create_array, Array, ArrayRef, BooleanArray, GenericStringArray, Int32Array, Int32Builder,
    ListArray, MapArray, MapBuilder, MapFieldNames, StringBuilder, StructArray,
};
use crate::arrow::buffer::{OffsetBuffer, ScalarBuffer};
use crate::arrow::datatypes::{DataType, Field, Fields, Schema};

use super::*;
use crate::expressions::*;
use crate::schema::{ArrayType, MapType, StructField, StructType};
use crate::DataType as DeltaDataTypes;
use crate::EvaluationHandlerExtension as _;

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

    let result = evaluate_predicate(&not_op, &batch).unwrap();
    let expected = BooleanArray::from(vec![true, false, true]);
    assert_eq!(result, expected);

    let result = evaluate_predicate(&in_op, &batch).unwrap();
    let expected = BooleanArray::from(vec![false, true, false]);
    assert_eq!(result, expected);
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

    let in_result = evaluate_predicate(&in_op, &batch);

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

    let in_op = Pred::not(Pred::binary(
        BinaryPredicateOp::In,
        Expr::literal(5),
        Scalar::Array(
            ArrayData::try_new(
                ArrayType::new(DeltaDataTypes::INTEGER, false),
                vec![Scalar::Integer(1), Scalar::Integer(2)],
            )
            .unwrap(),
        ),
    ));

    let in_result = evaluate_predicate(&in_op, &batch).unwrap();
    let in_expected = BooleanArray::from(vec![true]);
    assert_eq!(in_result, in_expected);
}

#[test]
fn test_literal_complex_type_array() {
    use crate::arrow::array::{Array as _, AsArray as _};
    use crate::arrow::datatypes::Int32Type;

    let array_type = ArrayType::new(DeltaDataTypes::INTEGER, true);
    let array_value = Scalar::Array(
        ArrayData::try_new(
            array_type.clone(),
            vec![
                Scalar::from(1),
                Scalar::from(2),
                Scalar::Null(DeltaDataTypes::INTEGER),
                Scalar::from(3),
            ],
        )
        .unwrap(),
    );
    let map_type = MapType::new(
        DeltaDataTypes::STRING,
        DeltaDataTypes::Array(Box::new(array_type.clone())),
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
        StructField::nullable("scalar", DeltaDataTypes::INTEGER),
        StructField::nullable("list", array_type.clone()),
        StructField::nullable("null_list", array_type.clone()),
        StructField::nullable("map", map_type.clone()),
        StructField::nullable("null_map", map_type.clone()),
    ];
    let struct_type = StructType::new(struct_fields.clone());
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

    let in_result = evaluate_predicate(&in_op, &batch);

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

    let result = evaluate_predicate(&str_in_op, &batch).unwrap();
    let expected = BooleanArray::from(vec![true, true, true]);
    assert_eq!(result, expected);

    let result = evaluate_predicate(&str_not_op, &batch).unwrap();
    let expected = BooleanArray::from(vec![false, false, false]);
    assert_eq!(result, expected);
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

    let predicate = column.clone().lt(Expr::literal(2));
    let results = evaluate_predicate(&predicate, &batch).unwrap();
    let expected = BooleanArray::from(vec![true, false, false]);
    assert_eq!(results, expected);

    let predicate = column.clone().le(Expr::literal(2));
    let results = evaluate_predicate(&predicate, &batch).unwrap();
    let expected = BooleanArray::from(vec![true, true, false]);
    assert_eq!(results, expected);

    let predicate = column.clone().gt(Expr::literal(2));
    let results = evaluate_predicate(&predicate, &batch).unwrap();
    let expected = BooleanArray::from(vec![false, false, true]);
    assert_eq!(results, expected);

    let predicate = column.clone().ge(Expr::literal(2));
    let results = evaluate_predicate(&predicate, &batch).unwrap();
    let expected = BooleanArray::from(vec![false, true, true]);
    assert_eq!(results, expected);

    let predicate = column.clone().eq(Expr::literal(2));
    let results = evaluate_predicate(&predicate, &batch).unwrap();
    let expected = BooleanArray::from(vec![false, true, false]);
    assert_eq!(results, expected);

    let predicate = column.clone().ne(Expr::literal(2));
    let results = evaluate_predicate(&predicate, &batch).unwrap();
    let expected = BooleanArray::from(vec![true, false, true]);
    assert_eq!(results, expected);
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
    let column_a = column_pred!("a");
    let column_b = column_pred!("b");

    let pred = Pred::and(column_a.clone(), column_b.clone());
    let results = evaluate_predicate(&pred, &batch).unwrap();
    let expected = BooleanArray::from(vec![false, false]);
    assert_eq!(results, expected);

    let pred = Pred::and(column_a.clone(), Pred::literal(true));
    let results = evaluate_predicate(&pred, &batch).unwrap();
    let expected = BooleanArray::from(vec![true, false]);
    assert_eq!(results, expected);

    let pred = Pred::or(column_a.clone(), column_b);
    let results = evaluate_predicate(&pred, &batch).unwrap();
    let expected = BooleanArray::from(vec![true, true]);
    assert_eq!(results, expected);

    let pred = Pred::or(column_a.clone(), Pred::literal(false));
    let results = evaluate_predicate(&pred, &batch).unwrap();
    let expected = BooleanArray::from(vec![true, false]);
    assert_eq!(results, expected);
}

#[test]
fn test_null_row() {
    // note that we _allow_ nested nulls, since the top-level struct can be NULL
    let schema = Arc::new(StructType::new(vec![
        StructField::nullable(
            "x",
            StructType::new([
                StructField::nullable("a", crate::schema::DataType::INTEGER),
                StructField::not_null("b", crate::schema::DataType::STRING),
            ]),
        ),
        StructField::nullable("c", crate::schema::DataType::STRING),
    ]));
    let handler = ArrowEvaluationHandler;
    let result = handler.null_row(schema.clone()).unwrap();
    let expected = RecordBatch::try_new(
        Arc::new(schema.as_ref().try_into().unwrap()),
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
    let not_null_schema = Arc::new(StructType::new(vec![StructField::not_null(
        "a",
        crate::schema::DataType::STRING,
    )]));
    let handler = ArrowEvaluationHandler;
    assert!(handler.null_row(not_null_schema).is_err());
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
        Scalar::Null(DeltaDataTypes::INTEGER),
    ];
    let schema = Arc::new(StructType::new([
        StructField::nullable("a", DeltaDataTypes::INTEGER),
        StructField::nullable("b", DeltaDataTypes::STRING),
        StructField::not_null("c", DeltaDataTypes::INTEGER),
        StructField::nullable("d", DeltaDataTypes::INTEGER),
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
    let schema = Arc::new(StructType::new([StructField::not_null(
        "a",
        DeltaDataTypes::struct_type([
            StructField::nullable("b", DeltaDataTypes::INTEGER),
            StructField::not_null("c", DeltaDataTypes::INTEGER),
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
    let values: &[Scalar] = &[Scalar::Null(DeltaDataTypes::INTEGER), 1.into()];
    let schema = Arc::new(StructType::new([StructField::not_null(
        "a",
        DeltaDataTypes::struct_type([
            StructField::nullable("b", DeltaDataTypes::INTEGER),
            StructField::not_null("c", DeltaDataTypes::INTEGER),
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
fn test_create_one_not_null_struct() {
    let values: &[Scalar] = &[
        Scalar::Null(DeltaDataTypes::INTEGER),
        Scalar::Null(DeltaDataTypes::INTEGER),
    ];
    let schema = Arc::new(StructType::new([StructField::not_null(
        "a",
        DeltaDataTypes::struct_type([
            StructField::not_null("b", DeltaDataTypes::INTEGER),
            StructField::nullable("c", DeltaDataTypes::INTEGER),
        ]),
    )]));
    let handler = ArrowEvaluationHandler;
    assert!(handler.create_one(schema, values).is_err());
}

#[test]
fn test_create_one_top_level_null() {
    let values = &[Scalar::Null(DeltaDataTypes::INTEGER)];
    let handler = ArrowEvaluationHandler;

    let schema = Arc::new(StructType::new([StructField::not_null(
        "col_1",
        DeltaDataTypes::INTEGER,
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
    let map_type = MapType::new(DeltaDataTypes::STRING, DeltaDataTypes::INTEGER, true);
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
    let map_type = MapType::new(DeltaDataTypes::STRING, DeltaDataTypes::STRING, false);
    let null_scalar_map = Scalar::Null(DeltaDataTypes::Map(Box::new(map_type)));
    let arrow_array = null_scalar_map.to_array(1)?;
    let map_array = arrow_array.as_any().downcast_ref::<MapArray>().unwrap();

    assert_eq!(map_array.len(), 1);
    assert_eq!(map_array.null_count(), 1);
    assert!(map_array.is_null(0));

    Ok(())
}
