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
    use std::sync::Arc;

    use datafusion_common::Column;
    use datafusion_expr::expr::{BinaryExpr, Case, InList};
    use datafusion_expr::{Expr, Operator};
    use delta_kernel::expressions::{
        column_expr, ArrayData, ColumnName, Expression as Expr_, Predicate as Pred, Scalar,
    };
    use delta_kernel::schema::{ArrayType, DataType, StructField, StructType};

    use super::{kernel_expr_to_df, kernel_pred_to_df};

    fn col(name: &str) -> Expr {
        Expr::Column(Column::new_unqualified(name))
    }

    fn lit_i64(v: i64) -> Expr {
        Expr::Literal(datafusion_common::ScalarValue::Int64(Some(v)), None)
    }

    fn lit_i32(v: i32) -> Expr {
        Expr::Literal(datafusion_common::ScalarValue::Int32(Some(v)), None)
    }

    fn lit_str(v: &str) -> Expr {
        Expr::Literal(
            datafusion_common::ScalarValue::Utf8(Some(v.to_string())),
            None,
        )
    }

    #[test]
    fn translates_top_level_column() {
        let kernel = column_expr!("version");
        let df = kernel_expr_to_df(&kernel).unwrap();
        assert_eq!(df, col("version"));
    }

    #[test]
    fn nested_column_translates_to_get_field_chain() {
        let kernel = Expr_::column(["add", "path"]);
        let df = kernel_expr_to_df(&kernel).unwrap();
        assert_eq!(format!("{df}"), "get_field(add, Utf8(\"path\"))");
    }

    #[test]
    fn translates_primitive_literals() {
        // i32, i64, string, bool
        assert_eq!(
            kernel_expr_to_df(&Expr_::literal(7i32)).unwrap(),
            lit_i32(7)
        );
        assert_eq!(
            kernel_expr_to_df(&Expr_::literal(42i64)).unwrap(),
            lit_i64(42)
        );
        assert_eq!(
            kernel_expr_to_df(&Expr_::literal("abc")).unwrap(),
            lit_str("abc")
        );
        assert_eq!(
            kernel_expr_to_df(&Expr_::literal(true)).unwrap(),
            Expr::Literal(datafusion_common::ScalarValue::Boolean(Some(true)), None)
        );
    }

    #[test]
    fn translates_typed_null_literal() {
        let kernel = Expr_::null_literal(DataType::LONG);
        assert_eq!(
            kernel_expr_to_df(&kernel).unwrap(),
            Expr::Literal(datafusion_common::ScalarValue::Int64(None), None)
        );
    }

    #[test]
    fn translates_arithmetic_binary() {
        // a + 5
        let kernel = column_expr!("a") + Expr_::literal(5i64);
        let df = kernel_expr_to_df(&kernel).unwrap();
        let expected = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("a")),
            Operator::Plus,
            Box::new(lit_i64(5)),
        ));
        assert_eq!(df, expected);
    }

    #[test]
    fn translates_predicate_eq_lt_gt() {
        // x == 1
        let p = column_expr!("x").eq(Expr_::literal(1i64));
        let df = kernel_pred_to_df(&p).unwrap();
        assert_eq!(
            df,
            Expr::BinaryExpr(BinaryExpr::new(
                Box::new(col("x")),
                Operator::Eq,
                Box::new(lit_i64(1))
            ))
        );

        // x < 1
        let p = column_expr!("x").lt(Expr_::literal(1i64));
        let df = kernel_pred_to_df(&p).unwrap();
        assert_eq!(
            df,
            Expr::BinaryExpr(BinaryExpr::new(
                Box::new(col("x")),
                Operator::Lt,
                Box::new(lit_i64(1))
            ))
        );

        // x > 1
        let p = column_expr!("x").gt(Expr_::literal(1i64));
        let df = kernel_pred_to_df(&p).unwrap();
        assert_eq!(
            df,
            Expr::BinaryExpr(BinaryExpr::new(
                Box::new(col("x")),
                Operator::Gt,
                Box::new(lit_i64(1))
            ))
        );
    }

    #[test]
    fn translates_distinct_to_is_distinct_from() {
        let p = column_expr!("x").distinct(Expr_::literal(1i64));
        let df = kernel_pred_to_df(&p).unwrap();
        assert_eq!(
            df,
            Expr::BinaryExpr(BinaryExpr::new(
                Box::new(col("x")),
                Operator::IsDistinctFrom,
                Box::new(lit_i64(1))
            ))
        );
    }

    #[test]
    fn translates_in_to_in_list() {
        // x IN (1, 2, 3)
        let arr = ArrayData::try_new(
            ArrayType::new(DataType::LONG, false),
            vec![Scalar::Long(1), Scalar::Long(2), Scalar::Long(3)],
        )
        .unwrap();
        let p = Pred::binary(
            delta_kernel::expressions::BinaryPredicateOp::In,
            column_expr!("x"),
            Expr_::literal(Scalar::Array(arr)),
        );
        let df = kernel_pred_to_df(&p).unwrap();
        let expected = Expr::InList(InList::new(
            Box::new(col("x")),
            vec![lit_i64(1), lit_i64(2), lit_i64(3)],
            false,
        ));
        assert_eq!(df, expected);
    }

    #[test]
    fn translates_is_null() {
        let p = column_expr!("x").is_null();
        let df = kernel_pred_to_df(&p).unwrap();
        assert_eq!(df, Expr::IsNull(Box::new(col("x"))));
    }

    #[test]
    fn translates_not_predicate() {
        let p = Pred::not(column_expr!("x").is_null());
        let df = kernel_pred_to_df(&p).unwrap();
        assert_eq!(df, Expr::Not(Box::new(Expr::IsNull(Box::new(col("x"))))));
    }

    #[test]
    fn translates_junction_and_or() {
        // a IS NULL AND b > 5
        let p = Pred::and(
            column_expr!("a").is_null(),
            column_expr!("b").gt(Expr_::literal(5i64)),
        );
        let df = kernel_pred_to_df(&p).unwrap();
        let expected = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::IsNull(Box::new(col("a")))),
            Operator::And,
            Box::new(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(col("b")),
                Operator::Gt,
                Box::new(lit_i64(5)),
            ))),
        ));
        assert_eq!(df, expected);

        // a IS NULL OR b IS NULL OR c IS NULL  -> left-associative chain
        let p = Pred::or_from([
            column_expr!("a").is_null(),
            column_expr!("b").is_null(),
            column_expr!("c").is_null(),
        ]);
        let df = kernel_pred_to_df(&p).unwrap();
        // Expect ((a IS NULL OR b IS NULL) OR c IS NULL)
        let inner = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::IsNull(Box::new(col("a")))),
            Operator::Or,
            Box::new(Expr::IsNull(Box::new(col("b")))),
        ));
        let expected = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(inner),
            Operator::Or,
            Box::new(Expr::IsNull(Box::new(col("c")))),
        ));
        assert_eq!(df, expected);
    }

    #[test]
    fn translates_if_to_case() {
        // IF(x IS NULL, 0, x)
        let kernel = Expr_::if_then_else(
            column_expr!("x").is_null(),
            Expr_::literal(0i64),
            column_expr!("x"),
        );
        let df = kernel_expr_to_df(&kernel).unwrap();
        let expected = Expr::Case(Case::new(
            None,
            vec![(
                Box::new(Expr::IsNull(Box::new(col("x")))),
                Box::new(lit_i64(0)),
            )],
            Some(Box::new(col("x"))),
        ));
        assert_eq!(df, expected);
    }

    #[test]
    fn coalesce_translates_to_datafusion_coalesce() {
        let kernel = Expr_::coalesce([column_expr!("a"), column_expr!("b"), column_expr!("c")]);
        let df = kernel_expr_to_df(&kernel).unwrap();
        assert_eq!(format!("{df}"), "coalesce(a, b, c)");
    }

    #[test]
    fn array_translates_to_make_array() {
        let kernel = Expr_::array([column_expr!("a"), column_expr!("b")]);
        let df = kernel_expr_to_df(&kernel).unwrap();
        assert_eq!(format!("{df}"), "make_array(a, b)");
    }

    #[test]
    fn struct_translates_to_df_struct_function() {
        let kernel = Expr_::struct_from([Arc::new(column_expr!("a")), Arc::new(column_expr!("b"))]);
        let df = kernel_expr_to_df(&kernel).unwrap();
        assert_eq!(format!("{df}"), "struct(a, b)");
    }

    #[test]
    fn parse_json_translates_to_json_get_and_named_struct() {
        let output_schema = Arc::new(
            StructType::try_new(vec![StructField::nullable("numRecords", DataType::LONG)]).unwrap(),
        );
        let kernel = Expr_::parse_json(column_expr!("stats"), output_schema);
        let df = kernel_expr_to_df(&kernel).unwrap();
        let lowered = format!("{df}");
        assert!(lowered.contains("named_struct"));
        assert!(lowered.contains("json_get_int"));
    }

    #[test]
    fn opaque_predicate_returns_unsupported() {
        // Build an Opaque via Predicate::Unknown which is the simpler case
        let p = Pred::unknown("mystery");
        let err = kernel_pred_to_df(&p).unwrap_err();
        assert!(format!("{err}").contains("Unknown"));
    }

    #[test]
    fn nested_if_lowers_correctly() {
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
        let df = kernel_expr_to_df(&kernel).unwrap();
        let inner = Expr::Case(Case::new(
            None,
            vec![(
                Box::new(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(col("a")),
                    Operator::Gt,
                    Box::new(lit_i64(100)),
                ))),
                Box::new(lit_i64(100)),
            )],
            Some(Box::new(col("a"))),
        ));
        let expected = Expr::Case(Case::new(
            None,
            vec![(
                Box::new(Expr::IsNull(Box::new(col("a")))),
                Box::new(lit_i64(0)),
            )],
            Some(Box::new(inner)),
        ));
        assert_eq!(df, expected);
    }

    #[test]
    fn complex_nested_predicate_round_trips_through_translator() {
        // (a + 1 > 5) AND NOT (b IS NULL)
        let p = Pred::and(
            (column_expr!("a") + Expr_::literal(1i64)).gt(Expr_::literal(5i64)),
            Pred::not(column_expr!("b").is_null()),
        );
        let df = kernel_pred_to_df(&p).unwrap();
        let lhs = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(col("a")),
                Operator::Plus,
                Box::new(lit_i64(1)),
            ))),
            Operator::Gt,
            Box::new(lit_i64(5)),
        ));
        let rhs = Expr::Not(Box::new(Expr::IsNull(Box::new(col("b")))));
        let expected =
            Expr::BinaryExpr(BinaryExpr::new(Box::new(lhs), Operator::And, Box::new(rhs)));
        assert_eq!(df, expected);
    }

    #[test]
    fn translates_multisegment_column_path_to_nested_get_field() {
        let name = ColumnName::new(["a", "b", "c"]);
        let df = kernel_expr_to_df(&Expr_::Column(name)).unwrap();
        assert_eq!(
            format!("{df}"),
            "get_field(get_field(a, Utf8(\"b\")), Utf8(\"c\"))"
        );
    }
}
