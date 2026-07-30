//! Conversion from a kernel [`Predicate`](KernelPredicate) to a boolean-valued DataFusion
//! [`Expr`](DFExpr).

use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::utils::{conjunction, disjunction};
use datafusion::logical_expr::{binary_expr, lit, Expr as DFExpr, Operator};
use delta_kernel::expressions::{
    BinaryPredicate as KernelBinaryPredicate, BinaryPredicateOp as KernelBinaryPredicateOp,
    Expression as KernelExpression, JunctionPredicate as KernelJunctionPredicate,
    JunctionPredicateOp as KernelJunctionPredicateOp, Predicate as KernelPredicate,
    Scalar as KernelScalar, UnaryPredicate as KernelUnaryPredicate,
    UnaryPredicateOp as KernelUnaryPredicateOp,
};
use delta_kernel::schema::StructType;
use delta_kernel::{DeltaResult, Error};

use crate::expression::to_df_expr;
use crate::scalar::to_df_scalar;

/// Converts a kernel [`Predicate`](KernelPredicate) into a boolean-valued DataFusion
/// [`Expr`](DFExpr), validating column references against `input_schema` (threaded to the
/// expression converter).
///
/// # Errors
/// Returns [`Error::unsupported`] for engine-defined (`Opaque`) or opaque-to-both (`Unknown`)
/// predicates. Also propagates any error from converting a child expression (an unresolved column
/// reference, or an interval literal, which has no Arrow representation) and rejects an `IN`
/// predicate whose right side is not a literal array.
pub fn to_df_predicate(pred: &KernelPredicate, input_schema: &StructType) -> DeltaResult<DFExpr> {
    match pred {
        KernelPredicate::BooleanExpression(expr) => to_df_expr(expr, input_schema),
        KernelPredicate::Not(inner) => {
            let df_inner = to_df_predicate(inner, input_schema)?;
            Ok(DFExpr::Not(Box::new(df_inner)))
        }
        KernelPredicate::Unary(unary) => unary_to_df_expr(unary, input_schema),
        KernelPredicate::Binary(binary) => binary_to_df_expr(binary, input_schema),
        KernelPredicate::Junction(junction) => junction_to_df_expr(junction, input_schema),
        KernelPredicate::Opaque(_) => Err(Error::unsupported(
            "cannot convert an engine-defined Opaque predicate",
        )),
        KernelPredicate::Unknown(name) => Err(Error::unsupported(format!(
            "cannot convert Unknown predicate {name:?}"
        ))),
    }
}

/// Lowers a unary predicate.
fn unary_to_df_expr(
    unary: &KernelUnaryPredicate,
    input_schema: &StructType,
) -> DeltaResult<DFExpr> {
    let expr = to_df_expr(&unary.expr, input_schema)?;
    Ok(match unary.op {
        KernelUnaryPredicateOp::IsNull => DFExpr::IsNull(Box::new(expr)),
    })
}

/// Lowers a binary predicate.
fn binary_to_df_expr(
    binary: &KernelBinaryPredicate,
    input_schema: &StructType,
) -> DeltaResult<DFExpr> {
    let op = match binary.op {
        KernelBinaryPredicateOp::In => {
            return in_to_df_expr(&binary.left, &binary.right, input_schema)
        }
        KernelBinaryPredicateOp::Equal => Operator::Eq,
        KernelBinaryPredicateOp::LessThan => Operator::Lt,
        KernelBinaryPredicateOp::GreaterThan => Operator::Gt,
        KernelBinaryPredicateOp::Distinct => Operator::IsDistinctFrom,
    };
    let left = to_df_expr(&binary.left, input_schema)?;
    let right = to_df_expr(&binary.right, input_schema)?;
    Ok(binary_expr(left, op, right))
}

/// Lowers an `IN` predicate. Kernel models `x IN (..)` as `Binary(In, value, literal_array)` with
/// the right side a constant `Scalar::Array`; DataFusion carries the list as a `Vec<Expr>` inside
/// `Expr::InList`. `NOT IN` reaches the converter as `Not(Binary(In, ..))`, so `negated` is always
/// `false` here.
///
/// # Errors
/// Returns [`Error::unsupported`] if the right operand is not a literal array.
fn in_to_df_expr(
    value: &KernelExpression,
    list: &KernelExpression,
    input_schema: &StructType,
) -> DeltaResult<DFExpr> {
    let KernelExpression::Literal(KernelScalar::Array(array)) = list else {
        return Err(Error::unsupported(
            "converting an IN predicate requires a literal array on the right-hand side",
        ));
    };
    let elements: DeltaResult<Vec<DFExpr>> = array
        .array_elements()
        .iter()
        .map(|scalar| Ok(lit(to_df_scalar(scalar)?)))
        .collect();
    let value = to_df_expr(value, input_schema)?;
    Ok(DFExpr::InList(InList::new(
        Box::new(value),
        elements?,
        false,
    )))
}

/// Lowers a junction (`And`/`Or`) by converting each child and combining them with DataFusion's
/// left-associative [`conjunction`]/[`disjunction`] helpers.
fn junction_to_df_expr(
    junction: &KernelJunctionPredicate,
    input_schema: &StructType,
) -> DeltaResult<DFExpr> {
    let preds: DeltaResult<Vec<DFExpr>> = junction
        .preds
        .iter()
        .map(|pred| to_df_predicate(pred, input_schema))
        .collect();
    let preds = preds?;
    Ok(match junction.op {
        // An empty junction lowers `AND` to `true` and `OR` to `false`, keeping kernel semantics
        KernelJunctionPredicateOp::And => conjunction(preds).unwrap_or_else(|| lit(true)),
        KernelJunctionPredicateOp::Or => disjunction(preds).unwrap_or_else(|| lit(false)),
    })
}

#[cfg(test)]
mod tests {
    use delta_kernel::expressions::{
        column_expr, ArrayData, Expression as Expr_, Predicate as Pred,
    };
    use delta_kernel::schema::{ArrayType, DataType, StructField};
    use rstest::rstest;

    use super::*;

    /// Name-resolution scope for these tests: top-level `a`, `b`, `c`, all `long`.
    fn test_schema() -> StructType {
        StructType::try_new([
            StructField::nullable("a", DataType::LONG),
            StructField::nullable("b", DataType::LONG),
            StructField::nullable("c", DataType::LONG),
        ])
        .unwrap()
    }

    /// Lowers a predicate against [`test_schema`] and renders it as a DataFusion `Display` string.
    fn lower(pred: Pred) -> String {
        to_df_predicate(&pred, &test_schema()).unwrap().to_string()
    }

    /// A literal `Scalar::Array` of longs, for `IN`-list cases.
    fn long_array(values: impl IntoIterator<Item = i64>) -> Expr_ {
        let elements: Vec<KernelScalar> = values.into_iter().map(KernelScalar::Long).collect();
        let array = ArrayData::try_new(ArrayType::new(DataType::LONG, false), elements).unwrap();
        Expr_::literal(KernelScalar::Array(array))
    }

    #[rstest]
    // Primitive comparisons lower to a native binary op.
    #[case::eq(column_expr!("a").eq(Expr_::literal(1i64)), "a = Int64(1)")]
    #[case::lt(column_expr!("a").lt(Expr_::literal(1i64)), "a < Int64(1)")]
    #[case::gt(column_expr!("a").gt(Expr_::literal(1i64)), "a > Int64(1)")]
    #[case::distinct(
        column_expr!("a").distinct(Expr_::literal(1i64)),
        "a IS DISTINCT FROM Int64(1)"
    )]
    // Kernel has no <=/>=/!= ops: each is `Not` of a primitive comparison, so it renders negated.
    #[case::ne(column_expr!("a").ne(Expr_::literal(1i64)), "NOT a = Int64(1)")]
    #[case::le(column_expr!("a").le(Expr_::literal(1i64)), "NOT a > Int64(1)")]
    #[case::ge(column_expr!("a").ge(Expr_::literal(1i64)), "NOT a < Int64(1)")]
    // Unary.
    #[case::is_null(column_expr!("a").is_null(), "a IS NULL")]
    #[case::is_not_null(column_expr!("a").is_not_null(), "NOT a IS NULL")]
    // IN / NOT IN.
    #[case::in_list(
        Pred::binary(KernelBinaryPredicateOp::In, column_expr!("a"), long_array([1, 2, 3])),
        "a IN ([Int64(1), Int64(2), Int64(3)])"
    )]
    #[case::not_in(
        Pred::not(Pred::binary(KernelBinaryPredicateOp::In, column_expr!("a"), long_array([1, 2]))),
        "NOT a IN ([Int64(1), Int64(2)])"
    )]
    // Junctions fold left-associatively.
    #[case::and(
        Pred::and(column_expr!("a").is_null(), column_expr!("b").is_null()),
        "a IS NULL AND b IS NULL"
    )]
    #[case::or(
        Pred::or(column_expr!("a").is_null(), column_expr!("b").is_null()),
        "a IS NULL OR b IS NULL"
    )]
    #[case::multi_and(
        Pred::and_from([
            column_expr!("a").is_null(),
            column_expr!("b").is_null(),
            column_expr!("c").is_null(),
        ]),
        "a IS NULL AND b IS NULL AND c IS NULL"
    )]
    // A bare boolean expression delegates straight to the expression converter.
    #[case::boolean_expression(Pred::from_expr(column_expr!("a")), "a")]
    // Nesting: predicates compose recursively. DataFusion's Display does not parenthesize the
    // junction under a `Not`, though the underlying `Expr` tree is still `Not(And(..))`.
    #[case::not_of_junction(
        Pred::not(Pred::and(column_expr!("a").is_null(), column_expr!("b").is_null())),
        "NOT a IS NULL AND b IS NULL"
    )]
    #[case::junction_of_junction(
        Pred::or(
            Pred::and(column_expr!("a").is_null(), column_expr!("b").is_null()),
            column_expr!("c").is_null(),
        ),
        "a IS NULL AND b IS NULL OR c IS NULL"
    )]
    #[case::and_of_comparisons(
        Pred::and(
            column_expr!("a").eq(Expr_::literal(1i64)),
            column_expr!("b").gt(Expr_::literal(2i64)),
        ),
        "a = Int64(1) AND b > Int64(2)"
    )]
    fn predicate_lowers_to_expected(#[case] kernel: Pred, #[case] expected: &str) {
        assert_eq!(lower(kernel), expected);
    }

    #[rstest]
    // Engine-defined and opaque-to-both predicates have no DataFusion equivalent.
    #[case::unknown(Pred::Unknown("mystery".into()))]
    // An `IN` whose right side is not a literal array cannot be lowered.
    #[case::in_without_literal_array(
        Pred::binary(KernelBinaryPredicateOp::In, column_expr!("a"), column_expr!("b"))
    )]
    fn unsupported_predicate_is_an_error(#[case] pred: Pred) {
        to_df_predicate(&pred, &test_schema()).unwrap_err();
    }
}
