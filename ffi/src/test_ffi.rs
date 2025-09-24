//! Utility functions used for testing ffi code

use std::sync::Arc;

use crate::expressions::{SharedExpression, SharedPredicate};
use crate::handle::Handle;
use delta_kernel::expressions::{
    column_expr, column_name, column_pred, ArrayData, BinaryExpressionOp, BinaryPredicateOp,
    Expression as Expr, MapData, OpaqueExpressionOp, OpaquePredicateOp, Predicate as Pred, Scalar,
    ScalarExpressionEvaluator, StructData, Transform,
};
use delta_kernel::kernel_predicates::{
    DirectDataSkippingPredicateEvaluator, DirectPredicateEvaluator,
    IndirectDataSkippingPredicateEvaluator,
};
use delta_kernel::schema::{ArrayType, DataType, MapType, StructField, StructType};
use delta_kernel::DeltaResult;

#[derive(Debug, PartialEq)]
struct OpaqueTestOp(String);

impl OpaqueExpressionOp for OpaqueTestOp {
    fn name(&self) -> &str {
        &self.0
    }
    fn eval_expr_scalar(
        &self,
        _eval_expr: &ScalarExpressionEvaluator<'_>,
        _exprs: &[Expr],
    ) -> DeltaResult<Scalar> {
        unimplemented!()
    }
}

impl OpaquePredicateOp for OpaqueTestOp {
    fn name(&self) -> &str {
        &self.0
    }

    fn eval_pred_scalar(
        &self,
        _eval_expr: &ScalarExpressionEvaluator<'_>,
        _evaluator: &DirectPredicateEvaluator<'_>,
        _exprs: &[Expr],
        _inverted: bool,
    ) -> DeltaResult<Option<bool>> {
        unimplemented!()
    }

    fn eval_as_data_skipping_predicate(
        &self,
        _evaluator: &DirectDataSkippingPredicateEvaluator<'_>,
        _exprs: &[Expr],
        _inverted: bool,
    ) -> Option<bool> {
        unimplemented!()
    }

    fn as_data_skipping_predicate(
        &self,
        _evaluator: &IndirectDataSkippingPredicateEvaluator<'_>,
        _exprs: &[Expr],
        _inverted: bool,
    ) -> Option<Pred> {
        unimplemented!()
    }
}

/// Constructs a kernel expression that is passed back as a [`SharedExpression`] handle. The expected
/// output expression can be found in `ffi/tests/test_expression_visitor/expected.txt`.
///
/// # Safety
/// The caller is responsible for freeing the returned memory, either by calling
/// [`crate::expressions::free_kernel_expression`], or [`crate::handle::Handle::drop_handle`].
#[no_mangle]
pub unsafe extern "C" fn get_testing_kernel_expression() -> Handle<SharedExpression> {
    let array_type = ArrayType::new(
        DataType::Primitive(delta_kernel::schema::PrimitiveType::Short),
        false,
    );
    let array_data =
        ArrayData::try_new(array_type.clone(), vec![Scalar::Short(5), Scalar::Short(0)]).unwrap();

    let map_type = MapType::new(DataType::STRING, DataType::STRING, false);
    let map_data = MapData::try_new(
        map_type.clone(),
        [
            ("key1".to_string(), "val1".to_string()),
            ("key2".to_string(), "val2".to_string()),
        ],
    )
    .unwrap();

    let nested_fields = vec![
        StructField::not_null("a", DataType::INTEGER),
        StructField::not_null("b", array_type),
    ];
    let nested_values = vec![Scalar::Integer(500), Scalar::Array(array_data.clone())];
    let nested_struct = StructData::try_new(nested_fields.clone(), nested_values).unwrap();
    let nested_struct_type = StructType::try_new(nested_fields).unwrap();

    let top_level_struct = StructData::try_new(
        vec![StructField::nullable(
            "top",
            DataType::Struct(Box::new(nested_struct_type)),
        )],
        vec![Scalar::Struct(nested_struct)],
    )
    .unwrap();

    let nested_transform = Transform::new_top_level()
        .with_dropped_field("gone")
        .with_replaced_field("stub", Expr::literal("replaced").into())
        .with_inserted_field(Some("x".to_string()), Expr::literal(true).into())
        .with_inserted_field(Some("y".to_string()), Expr::literal(false).into());
    let top_level_transform = Transform::new_nested(column_name!("foo.bar.baz"))
        .with_dropped_field("dropme")
        .with_replaced_field("replaceme", Expr::literal(42).into())
        .with_inserted_field(None::<&str>, Expr::literal("prepended").into())
        .with_inserted_field(Some("a".to_string()), Expr::literal("first").into())
        .with_inserted_field(
            Some("a".to_string()),
            Expr::transform(nested_transform).into(),
        )
        .with_inserted_field(Some("a".to_string()), Expr::literal("third").into());

    let mut sub_exprs = vec![
        column_expr!("col"),
        Expr::literal(i8::MAX),
        Expr::literal(i8::MIN),
        Expr::literal(f32::MAX),
        Expr::literal(f32::MIN),
        Expr::literal(f64::MAX),
        Expr::literal(f64::MIN),
        Expr::literal(i32::MAX),
        Expr::literal(i32::MIN),
        Expr::literal(i64::MAX),
        Expr::literal(i64::MIN),
        Expr::literal("hello expressions"),
        Expr::literal(true),
        Expr::literal(false),
        Scalar::Timestamp(50).into(),
        Scalar::TimestampNtz(100).into(),
        Scalar::Date(32).into(),
        Scalar::Binary(0x0000deadbeefcafeu64.to_be_bytes().to_vec()).into(),
        // Both the most and least significant u64 of the Decimal value will be 1
        Scalar::decimal((1i128 << 64) + 1, 20, 3).unwrap().into(),
        Expr::null_literal(DataType::SHORT),
        Scalar::Struct(top_level_struct).into(),
        Expr::Transform(top_level_transform),
        Scalar::Array(array_data).into(),
        Scalar::Map(map_data).into(),
        Expr::struct_from([Expr::literal(5_i32), Expr::literal(20_i64)]),
        Expr::opaque(
            OpaqueTestOp("foo".to_string()),
            vec![Expr::literal(42), Expr::literal(1.111)],
        ),
        Expr::unknown("mystery"),
    ];
    sub_exprs.extend(
        [
            BinaryExpressionOp::Divide,
            BinaryExpressionOp::Multiply,
            BinaryExpressionOp::Plus,
            BinaryExpressionOp::Minus,
        ]
        .into_iter()
        .map(|op| Expr::binary(op, Expr::literal(0), Expr::literal(0))),
    );

    Arc::new(Expr::struct_from(sub_exprs)).into()
}

/// Constructs a kernel predicate that is passed back as a [`SharedPredicate`] handle. The expected
/// output predicate can be found in `ffi/tests/test_predicate_visitor/expected.txt`.
///
/// # Safety
/// The caller is responsible for freeing the returned memory, either by calling
/// [`crate::expressions::free_kernel_predicate`], or [`crate::handle::Handle::drop_handle`].
#[no_mangle]
pub unsafe extern "C" fn get_testing_kernel_predicate() -> Handle<SharedPredicate> {
    let array_type = ArrayType::new(
        DataType::Primitive(delta_kernel::schema::PrimitiveType::Short),
        false,
    );
    let array_data =
        ArrayData::try_new(array_type.clone(), vec![Scalar::Short(5), Scalar::Short(0)]).unwrap();

    let mut sub_exprs = vec![
        column_pred!("col"),
        Pred::literal(true),
        Pred::literal(false),
        Pred::binary(
            BinaryPredicateOp::In,
            Expr::literal(10),
            Scalar::Array(array_data.clone()),
        ),
        Pred::not(Pred::binary(
            BinaryPredicateOp::In,
            Expr::literal(10),
            Scalar::Array(array_data),
        )),
        Pred::or_from(vec![
            Pred::eq(Expr::literal(5), Expr::literal(10)),
            Pred::ne(Expr::literal(20), Expr::literal(10)),
        ]),
        Pred::is_not_null(column_expr!("col")),
        Pred::opaque(
            OpaqueTestOp("bar".to_string()),
            vec![Expr::literal(42), Expr::literal(1.111)],
        ),
        Pred::unknown("intrigue"),
    ];
    sub_exprs.extend(
        [
            Pred::eq,
            Pred::ne,
            Pred::lt,
            Pred::le,
            Pred::gt,
            Pred::ge,
            Pred::distinct,
        ]
        .into_iter()
        .map(|op_fn| op_fn(Expr::literal(0), Expr::literal(0))),
    );

    Arc::new(Pred::and_from(sub_exprs)).into()
}
