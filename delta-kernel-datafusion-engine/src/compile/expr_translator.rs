//! Kernel [`Expression`] / [`Predicate`] -> DataFusion [`Expr`] translation.
//!
//! This is a **partial** translator; it covers the subset required by FSR's `Window`
//! `partition_by` / `order_by` and the eventual lowering of `Filter` / `Project` / `Join` keys to
//! native DataFusion logical plans. Each unsupported variant returns
//! [`crate::error::unsupported`] with a clear error rather than silently producing an
//! incorrect plan.
//!
//! Supported today:
//! - `Literal` (all primitive scalar types + typed NULL)
//! - `Column` (top-level + nested via DataFusion `get_field`)
//! - `Predicate` wrappers (BooleanExpression, Not, Junction And/Or, Unary IsNull, Binary
//!   Eq/Lt/Gt/Distinct/In)
//! - `Binary` arithmetic (Plus, Minus, Multiply, Divide)
//! - `Variadic(Coalesce)` (lowered to a nested CASE chain so we don't depend on the
//!   datafusion-functions `coalesce` UDF)
//! - `If` (lowered to `Expr::Case`)
//!
//! Deferred (returns Unsupported with TODO):
//! - `Struct` with nullability predicates, `Transform`, `Unary(ToJson)`, `MapToStruct`
//! - `Opaque`, `Unknown`
//! - `Predicate::Opaque`, `Predicate::Unknown`

use std::sync::Arc;

use datafusion_common::arrow::array::StructArray;
use datafusion_common::arrow::datatypes::Field as ArrowField;
use datafusion_common::error::DataFusionError;
use datafusion_common::{Column, ScalarValue};
use datafusion_expr::expr::{BinaryExpr, Case, InList};
use datafusion_expr::{Expr, Operator};
use datafusion_functions::core::expr_fn::{get_field, r#struct as make_struct};
use datafusion_functions_nested::expr_fn::make_array;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::expressions::{
    BinaryExpression, BinaryExpressionOp, BinaryPredicate, BinaryPredicateOp, ColumnName,
    Expression, IfExpression, JunctionPredicate, JunctionPredicateOp, Predicate, Scalar,
    StructData, UnaryPredicate, UnaryPredicateOp, VariadicExpression, VariadicExpressionOp,
};
use delta_kernel::schema::{DataType, PrimitiveType};

use crate::error::unsupported;

/// Build a DataFusion `Expr::BinaryExpr(l <op> r)` without the per-call `Box::new` ceremony.
fn binary(l: Expr, op: Operator, r: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(Box::new(l), op, Box::new(r)))
}

/// Translate a kernel [`Expression`] to a DataFusion [`Expr`].
pub fn kernel_expr_to_df(expr: &Expression) -> Result<Expr, DataFusionError> {
    match expr {
        Expression::Literal(scalar) => scalar_value_to_df(scalar).map(|v| Expr::Literal(v, None)),
        Expression::Column(name) => column_to_df(name),
        Expression::Predicate(pred) => kernel_pred_to_df(pred),
        Expression::Binary(BinaryExpression { op, left, right }) => {
            binary_expr_to_df(*op, left, right)
        }
        Expression::Variadic(VariadicExpression { op, exprs }) => variadic_to_df(*op, exprs),
        Expression::If(if_expr) => if_to_df(if_expr),
        Expression::Struct(children, nullability_predicate) => {
            if nullability_predicate.is_some() {
                return Err(unsupported(
                    "expr_translator: Struct with nullability predicate is not yet supported",
                ));
            }
            let args = children
                .iter()
                .map(|e| kernel_expr_to_df(e.as_ref()))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(make_struct(args))
        }
        Expression::Transform(_) => Err(unsupported(
            "expr_translator: Transform expressions are not yet supported",
        )),
        Expression::Unary(_) => Err(unsupported(
            "expr_translator: Unary(ToJson) is not yet supported",
        )),
        Expression::ParseJson(parse_json) => {
            let json_expr = kernel_expr_to_df(parse_json.json_expr.as_ref())?;
            let extracted = crate::compile::json_parse::generate_schema_extractions(
                &json_expr,
                &parse_json.output_schema,
            )?;
            let mut args = Vec::with_capacity(extracted.len() * 2);
            for (field_expr, field_name) in extracted {
                args.push(Expr::Literal(ScalarValue::Utf8(Some(field_name)), None));
                args.push(field_expr);
            }
            Ok(datafusion_functions::core::expr_fn::named_struct(args))
        }
        Expression::MapToStruct(_) => Err(unsupported(
            "expr_translator: MapToStruct is not yet supported",
        )),
        Expression::Opaque(_) => Err(unsupported(
            "expr_translator: Opaque expressions are not supported (engine-defined ops do not \
             round-trip through DataFusion logical plans)",
        )),
        Expression::Unknown(name) => Err(unsupported(format!(
            "expr_translator: Unknown expression {name:?} cannot be translated"
        ))),
    }
}

/// Translate a kernel [`Predicate`] to a boolean-typed DataFusion [`Expr`].
pub fn kernel_pred_to_df(pred: &Predicate) -> Result<Expr, DataFusionError> {
    match pred {
        Predicate::BooleanExpression(expr) => kernel_expr_to_df(expr),
        Predicate::Not(inner) => Ok(Expr::Not(Box::new(kernel_pred_to_df(inner)?))),
        Predicate::Unary(UnaryPredicate { op, expr }) => unary_pred_to_df(*op, expr),
        Predicate::Binary(BinaryPredicate { op, left, right }) => {
            binary_pred_to_df(*op, left, right)
        }
        Predicate::Junction(JunctionPredicate { op, preds }) => junction_to_df(*op, preds),
        Predicate::Opaque(_) => Err(unsupported(
            "expr_translator: Opaque predicates are not supported",
        )),
        Predicate::Unknown(name) => Err(unsupported(format!(
            "expr_translator: Unknown predicate {name:?} cannot be translated"
        ))),
    }
}

fn column_to_df(name: &ColumnName) -> Result<Expr, DataFusionError> {
    let mut parts = name.iter();
    let root = parts
        .next()
        .ok_or_else(|| unsupported("expr_translator: empty column path cannot be translated"))?;
    Ok(parts.fold(
        Expr::Column(Column::new_unqualified(root)),
        |expr, field| get_field(expr, field.as_str()),
    ))
}

fn binary_expr_to_df(
    op: BinaryExpressionOp,
    left: &Expression,
    right: &Expression,
) -> Result<Expr, DataFusionError> {
    let df_op = match op {
        BinaryExpressionOp::Plus => Operator::Plus,
        BinaryExpressionOp::Minus => Operator::Minus,
        BinaryExpressionOp::Multiply => Operator::Multiply,
        BinaryExpressionOp::Divide => Operator::Divide,
    };
    Ok(binary(
        kernel_expr_to_df(left)?,
        df_op,
        kernel_expr_to_df(right)?,
    ))
}

fn unary_pred_to_df(op: UnaryPredicateOp, expr: &Expression) -> Result<Expr, DataFusionError> {
    let inner = kernel_expr_to_df(expr)?;
    match op {
        UnaryPredicateOp::IsNull => Ok(Expr::IsNull(Box::new(inner))),
    }
}

fn binary_pred_to_df(
    op: BinaryPredicateOp,
    left: &Expression,
    right: &Expression,
) -> Result<Expr, DataFusionError> {
    // `In` is special: kernel models it as `Binary(In, value, array_literal)` where the right side
    // is a constant `Scalar::Array`. DataFusion's `Expr::InList` carries the list as a Vec<Expr>.
    let df_op = match op {
        BinaryPredicateOp::In => return in_pred_to_df(left, right),
        BinaryPredicateOp::Equal => Operator::Eq,
        BinaryPredicateOp::LessThan => Operator::Lt,
        BinaryPredicateOp::GreaterThan => Operator::Gt,
        BinaryPredicateOp::Distinct => Operator::IsDistinctFrom,
    };
    Ok(binary(
        kernel_expr_to_df(left)?,
        df_op,
        kernel_expr_to_df(right)?,
    ))
}

fn in_pred_to_df(value: &Expression, list: &Expression) -> Result<Expr, DataFusionError> {
    let Expression::Literal(Scalar::Array(arr)) = list else {
        return Err(unsupported(format!(
            "expr_translator: IN predicate requires a literal array on the right; got {list:?}"
        )));
    };
    let elements = arr
        .array_elements()
        .iter()
        .map(|s| scalar_value_to_df(s).map(|v| Expr::Literal(v, None)))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Expr::InList(InList::new(
        Box::new(kernel_expr_to_df(value)?),
        elements,
        false,
    )))
}

fn junction_to_df(op: JunctionPredicateOp, preds: &[Predicate]) -> Result<Expr, DataFusionError> {
    let df_op = match op {
        JunctionPredicateOp::And => Operator::And,
        JunctionPredicateOp::Or => Operator::Or,
    };
    let mut iter = preds.iter().map(kernel_pred_to_df);
    let first = iter.next().ok_or_else(|| {
        unsupported("expr_translator: empty Junction (And/Or) cannot be lowered")
    })??;
    iter.try_fold(first, |acc, next| Ok(binary(acc, df_op, next?)))
}

fn variadic_to_df(op: VariadicExpressionOp, exprs: &[Expression]) -> Result<Expr, DataFusionError> {
    let args: Vec<Expr> = exprs
        .iter()
        .map(kernel_expr_to_df)
        .collect::<Result<_, _>>()?;
    match op {
        VariadicExpressionOp::Coalesce if args.is_empty() => Err(unsupported(
            "expr_translator: COALESCE() requires at least one argument",
        )),
        VariadicExpressionOp::Coalesce => Ok(datafusion_functions::core::expr_fn::coalesce(args)),
        VariadicExpressionOp::Array => Ok(make_array(args)),
    }
}

fn if_to_df(if_expr: &IfExpression) -> Result<Expr, DataFusionError> {
    let cond = Box::new(kernel_pred_to_df(&if_expr.condition)?);
    let then = Box::new(kernel_expr_to_df(&if_expr.then_expr)?);
    let r#else = Box::new(kernel_expr_to_df(&if_expr.else_expr)?);
    Ok(Expr::Case(Case::new(
        None,
        vec![(cond, then)],
        Some(r#else),
    )))
}

fn scalar_value_to_df(scalar: &Scalar) -> Result<ScalarValue, DataFusionError> {
    Ok(match scalar {
        Scalar::Integer(v) => ScalarValue::Int32(Some(*v)),
        Scalar::Long(v) => ScalarValue::Int64(Some(*v)),
        Scalar::Short(v) => ScalarValue::Int16(Some(*v)),
        Scalar::Byte(v) => ScalarValue::Int8(Some(*v)),
        Scalar::Float(v) => ScalarValue::Float32(Some(*v)),
        Scalar::Double(v) => ScalarValue::Float64(Some(*v)),
        Scalar::String(v) => ScalarValue::Utf8(Some(v.clone())),
        Scalar::Boolean(v) => ScalarValue::Boolean(Some(*v)),
        Scalar::Date(v) => ScalarValue::Date32(Some(*v)),
        Scalar::Timestamp(v) => ScalarValue::TimestampMicrosecond(Some(*v), Some(Arc::from("UTC"))),
        Scalar::TimestampNtz(v) => ScalarValue::TimestampMicrosecond(Some(*v), None),
        Scalar::Binary(v) => ScalarValue::Binary(Some(v.clone())),
        Scalar::Decimal(d) => {
            ScalarValue::Decimal128(Some(d.bits()), d.ty().precision(), d.ty().scale() as i8)
        }
        Scalar::Null(dt) => typed_null_to_df(dt)?,
        Scalar::Struct(v) => scalar_struct_value_to_df(v)?,
        Scalar::Array(_) | Scalar::Map(_) => {
            return Err(unsupported(format!(
                "expr_translator: complex literal scalar {:?} is not yet supported",
                scalar.data_type()
            )))
        }
    })
}

fn scalar_struct_value_to_df(struct_data: &StructData) -> Result<ScalarValue, DataFusionError> {
    let fields: Vec<ArrowField> = struct_data
        .fields()
        .iter()
        .map(|f| {
            f.try_into_arrow().map_err(|e| {
                unsupported(format!(
                    "expr_translator: struct literal field `{}` conversion failed: {e}",
                    f.name()
                ))
            })
        })
        .collect::<Result<_, _>>()?;
    let values = struct_data
        .values()
        .iter()
        .map(|v| scalar_value_to_df(v)?.to_array())
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ScalarValue::Struct(Arc::new(StructArray::new(
        fields.into(),
        values,
        None,
    ))))
}

fn typed_null_to_df(data_type: &DataType) -> Result<ScalarValue, DataFusionError> {
    if let DataType::Primitive(p) = data_type {
        return Ok(match p {
            PrimitiveType::Integer => ScalarValue::Int32(None),
            PrimitiveType::Long => ScalarValue::Int64(None),
            PrimitiveType::Short => ScalarValue::Int16(None),
            PrimitiveType::Byte => ScalarValue::Int8(None),
            PrimitiveType::Float => ScalarValue::Float32(None),
            PrimitiveType::Double => ScalarValue::Float64(None),
            PrimitiveType::String => ScalarValue::Utf8(None),
            PrimitiveType::Boolean => ScalarValue::Boolean(None),
            PrimitiveType::Date => ScalarValue::Date32(None),
            PrimitiveType::Timestamp => {
                ScalarValue::TimestampMicrosecond(None, Some(Arc::from("UTC")))
            }
            PrimitiveType::TimestampNtz => ScalarValue::TimestampMicrosecond(None, None),
            PrimitiveType::Binary => ScalarValue::Binary(None),
            PrimitiveType::Decimal(d) => {
                ScalarValue::Decimal128(None, d.precision(), d.scale() as i8)
            }
        });
    }

    let arrow_dt: datafusion_common::arrow::datatypes::DataType =
        data_type.try_into_arrow().map_err(|e| {
            unsupported(format!(
                "expr_translator: typed NULL conversion failed for {data_type:?}: {e}"
            ))
        })?;
    ScalarValue::try_from(&arrow_dt).map_err(|e| {
        unsupported(format!(
            "expr_translator: typed NULL for {data_type:?} is not supported by DataFusion: {e}"
        ))
    })
}

#[cfg(test)]
mod tests {
    //! Lowering assertions for [`super::kernel_expr_to_df`] / [`super::kernel_pred_to_df`].
    //!
    //! Assertions compare DataFusion's [`Display`](std::fmt::Display) output rather than
    //! hand-built `Expr` trees. The hand-built form tautologically re-encoded the lowering
    //! rule in test code (any change to the lowering would change both sides identically),
    //! and the printed form is shorter and self-documenting.
    use std::sync::Arc;

    use delta_kernel::expressions::{
        column_expr, ArrayData, BinaryPredicateOp, ColumnName, Expression as Expr_,
        Predicate as Pred, Scalar,
    };
    use delta_kernel::schema::{ArrayType, DataType, StructField, StructType};
    use rstest::rstest;

    use super::{kernel_expr_to_df, kernel_pred_to_df};

    fn lower_expr(e: Expr_) -> String {
        format!("{}", kernel_expr_to_df(&e).unwrap())
    }
    fn lower_pred(p: Pred) -> String {
        format!("{}", kernel_pred_to_df(&p).unwrap())
    }

    #[rstest]
    #[case::depth_2(Expr_::column(["add", "path"]), "get_field(add, Utf8(\"path\"))")]
    #[case::depth_3(
        Expr_::Column(ColumnName::new(["a", "b", "c"])),
        "get_field(get_field(a, Utf8(\"b\")), Utf8(\"c\"))"
    )]
    fn nested_column_lowers_to_get_field_chain(#[case] kernel: Expr_, #[case] expected: &str) {
        assert_eq!(lower_expr(kernel), expected);
    }

    #[rstest]
    #[case::i32(Expr_::literal(7i32), "Int32(7)")]
    #[case::i64(Expr_::literal(42i64), "Int64(42)")]
    #[case::string(Expr_::literal("abc"), "Utf8(\"abc\")")]
    #[case::bool(Expr_::literal(true), "Boolean(true)")]
    #[case::null_long(Expr_::null_literal(DataType::LONG), "Int64(NULL)")]
    fn translates_primitive_literals(#[case] kernel: Expr_, #[case] expected: &str) {
        assert_eq!(lower_expr(kernel), expected);
    }

    #[rstest]
    #[case::eq(column_expr!("x").eq(Expr_::literal(1i64)), "x = Int64(1)")]
    #[case::lt(column_expr!("x").lt(Expr_::literal(1i64)), "x < Int64(1)")]
    #[case::gt(column_expr!("x").gt(Expr_::literal(1i64)), "x > Int64(1)")]
    #[case::distinct(
        column_expr!("x").distinct(Expr_::literal(1i64)),
        "x IS DISTINCT FROM Int64(1)"
    )]
    fn translates_binary_predicates(#[case] kernel: Pred, #[case] expected: &str) {
        assert_eq!(lower_pred(kernel), expected);
    }

    #[test]
    fn translates_arithmetic_binary() {
        assert_eq!(
            lower_expr(column_expr!("a") + Expr_::literal(5i64)),
            "a + Int64(5)"
        );
    }

    #[test]
    fn translates_in_to_in_list() {
        let arr = ArrayData::try_new(
            ArrayType::new(DataType::LONG, false),
            vec![Scalar::Long(1), Scalar::Long(2), Scalar::Long(3)],
        )
        .unwrap();
        let p = Pred::binary(
            BinaryPredicateOp::In,
            column_expr!("x"),
            Expr_::literal(Scalar::Array(arr)),
        );
        assert_eq!(lower_pred(p), "x IN ([Int64(1), Int64(2), Int64(3)])");
    }

    #[test]
    fn translates_not_predicate() {
        assert_eq!(
            lower_pred(Pred::not(column_expr!("x").is_null())),
            "NOT x IS NULL"
        );
    }

    #[test]
    fn translates_junction_and_or_left_associative() {
        // AND: IsNull + Gt
        assert_eq!(
            lower_pred(Pred::and(
                column_expr!("a").is_null(),
                column_expr!("b").gt(Expr_::literal(5i64)),
            )),
            "a IS NULL AND b > Int64(5)"
        );
        // OR_from chain: ((a IS NULL OR b IS NULL) OR c IS NULL).
        assert_eq!(
            lower_pred(Pred::or_from([
                column_expr!("a").is_null(),
                column_expr!("b").is_null(),
                column_expr!("c").is_null(),
            ])),
            "a IS NULL OR b IS NULL OR c IS NULL"
        );
    }

    #[test]
    fn translates_if_to_case() {
        let kernel = Expr_::if_then_else(
            column_expr!("x").is_null(),
            Expr_::literal(0i64),
            column_expr!("x"),
        );
        assert_eq!(
            lower_expr(kernel),
            "CASE WHEN x IS NULL THEN Int64(0) ELSE x END"
        );
    }

    #[test]
    fn nested_if_lowers_to_nested_case() {
        // IF(a IS NULL, 0, IF(a > 100, 100, a)) -- clamp pattern
        let kernel = Expr_::if_then_else(
            column_expr!("a").is_null(),
            Expr_::literal(0i64),
            Expr_::if_then_else(
                column_expr!("a").gt(Expr_::literal(100i64)),
                Expr_::literal(100i64),
                column_expr!("a"),
            ),
        );
        assert_eq!(
            lower_expr(kernel),
            "CASE WHEN a IS NULL THEN Int64(0) ELSE \
             CASE WHEN a > Int64(100) THEN Int64(100) ELSE a END END"
        );
    }

    #[test]
    fn complex_nested_predicate_round_trips() {
        // (a + 1 > 5) AND NOT (b IS NULL)
        let p = Pred::and(
            (column_expr!("a") + Expr_::literal(1i64)).gt(Expr_::literal(5i64)),
            Pred::not(column_expr!("b").is_null()),
        );
        assert_eq!(lower_pred(p), "a + Int64(1) > Int64(5) AND NOT b IS NULL");
    }

    #[test]
    fn parse_json_translates_to_json_get_and_named_struct() {
        let output_schema = Arc::new(
            StructType::try_new(vec![StructField::nullable("numRecords", DataType::LONG)]).unwrap(),
        );
        let lowered = lower_expr(Expr_::parse_json(column_expr!("stats"), output_schema));
        assert!(lowered.contains("named_struct"), "{lowered}");
        assert!(lowered.contains("json_get_int"), "{lowered}");
    }

    #[test]
    fn opaque_predicate_returns_unsupported() {
        let err = kernel_pred_to_df(&Pred::unknown("mystery")).unwrap_err();
        assert!(format!("{err}").contains("Unknown"));
    }
}
