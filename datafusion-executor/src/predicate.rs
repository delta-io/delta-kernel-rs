//! Conversion from a kernel [`Predicate`](KernelPredicate) to a boolean-valued DataFusion
//! [`Expr`](DFExpr).

use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::utils::{conjunction, disjunction};
use datafusion::logical_expr::{binary_expr, lit, Expr as DFExpr, Operator};
use delta_kernel::expressions::{
    BinaryPredicate, BinaryPredicateOp, Expression as KernelExpression, JunctionPredicate,
    JunctionPredicateOp, Predicate as KernelPredicate, Scalar as KernelScalar, UnaryPredicate,
    UnaryPredicateOp,
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
/// predicates Also propagates any error from converting a child expression (an unresolved column
/// reference, or an interval literal, which has no Arrow representation) and rejects an `IN`
/// predicate whose right side is not a literal array.
pub fn to_df_predicate(pred: &KernelPredicate, input_schema: &StructType) -> DeltaResult<DFExpr> {
    match pred {
        KernelPredicate::BooleanExpression(expr) => to_df_expr(expr, input_schema),
        KernelPredicate::Not(inner) => {
            Ok(DFExpr::Not(Box::new(to_df_predicate(inner, input_schema)?)))
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
fn unary_to_df_expr(unary: &UnaryPredicate, input_schema: &StructType) -> DeltaResult<DFExpr> {
    let expr = to_df_expr(&unary.expr, input_schema)?;
    Ok(match unary.op {
        UnaryPredicateOp::IsNull => DFExpr::IsNull(Box::new(expr)),
    })
}

/// Lowers a binary predicate.
fn binary_to_df_expr(binary: &BinaryPredicate, input_schema: &StructType) -> DeltaResult<DFExpr> {
    let op = match binary.op {
        BinaryPredicateOp::In => return in_to_df_expr(&binary.left, &binary.right, input_schema),
        BinaryPredicateOp::Equal => Operator::Eq,
        BinaryPredicateOp::LessThan => Operator::Lt,
        BinaryPredicateOp::GreaterThan => Operator::Gt,
        BinaryPredicateOp::Distinct => Operator::IsDistinctFrom,
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
    let elements = array
        .array_elements()
        .iter()
        .map(|scalar| Ok(lit(to_df_scalar(scalar)?)))
        .collect::<DeltaResult<Vec<_>>>()?;
    let value = to_df_expr(value, input_schema)?;
    Ok(DFExpr::InList(InList::new(
        Box::new(value),
        elements,
        false,
    )))
}

/// Lowers a junction (`And`/`Or`) by converting each child and combining them with DataFusion's
/// left-associative [`conjunction`]/[`disjunction`] helpers.
fn junction_to_df_expr(
    junction: &JunctionPredicate,
    input_schema: &StructType,
) -> DeltaResult<DFExpr> {
    let preds = junction
        .preds
        .iter()
        .map(|pred| to_df_predicate(pred, input_schema))
        .collect::<DeltaResult<Vec<_>>>()?;
    Ok(match junction.op {
        // An empty junction lowers `AND` to `true` and `OR` to `false`, keeping kernel semantics
        JunctionPredicateOp::And => conjunction(preds).unwrap_or_else(|| lit(true)),
        JunctionPredicateOp::Or => disjunction(preds).unwrap_or_else(|| lit(false)),
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

    #[rstest]
    #[case::eq(column_expr!("a").eq(Expr_::literal(1i64)), "a = Int64(1)")]
    #[case::lt(column_expr!("a").lt(Expr_::literal(1i64)), "a < Int64(1)")]
    #[case::gt(column_expr!("a").gt(Expr_::literal(1i64)), "a > Int64(1)")]
    #[case::distinct(
        column_expr!("a").distinct(Expr_::literal(1i64)),
        "a IS DISTINCT FROM Int64(1)"
    )]
    fn comparison_lowers_to_binary_expr(#[case] kernel: Pred, #[case] expected: &str) {
        assert_eq!(lower(kernel), expected);
    }

    // Kernel has no <=/>=/!= operators: they are Not of a primitive comparison, so they render as
    // a negated form rather than a native LtEq/GtEq/NotEq.
    #[rstest]
    #[case::ne(column_expr!("a").ne(Expr_::literal(1i64)), "NOT a = Int64(1)")]
    #[case::le(column_expr!("a").le(Expr_::literal(1i64)), "NOT a > Int64(1)")]
    #[case::ge(column_expr!("a").ge(Expr_::literal(1i64)), "NOT a < Int64(1)")]
    fn derived_comparison_lowers_to_negated_primitive(
        #[case] kernel: Pred,
        #[case] expected: &str,
    ) {
        assert_eq!(lower(kernel), expected);
    }

    #[test]
    fn is_null_lowers_to_is_null() {
        assert_eq!(lower(column_expr!("a").is_null()), "a IS NULL");
    }

    #[test]
    fn is_not_null_lowers_to_negated_is_null() {
        assert_eq!(lower(column_expr!("a").is_not_null()), "NOT a IS NULL");
    }

    #[test]
    fn in_lowers_to_in_list() {
        let array = ArrayData::try_new(
            ArrayType::new(DataType::LONG, false),
            vec![
                KernelScalar::Long(1),
                KernelScalar::Long(2),
                KernelScalar::Long(3),
            ],
        )
        .unwrap();
        let kernel = Pred::binary(
            BinaryPredicateOp::In,
            column_expr!("a"),
            Expr_::literal(KernelScalar::Array(array)),
        );
        assert_eq!(lower(kernel), "a IN ([Int64(1), Int64(2), Int64(3)])");
    }

    #[test]
    fn not_in_lowers_to_negated_in_list() {
        let array = ArrayData::try_new(
            ArrayType::new(DataType::LONG, false),
            vec![KernelScalar::Long(1), KernelScalar::Long(2)],
        )
        .unwrap();
        let inner = Pred::binary(
            BinaryPredicateOp::In,
            column_expr!("a"),
            Expr_::literal(KernelScalar::Array(array)),
        );
        assert_eq!(lower(Pred::not(inner)), "NOT a IN ([Int64(1), Int64(2)])");
    }

    #[rstest]
    #[case::and(
        Pred::and(column_expr!("a").is_null(), column_expr!("b").is_null()),
        "a IS NULL AND b IS NULL"
    )]
    #[case::or(
        Pred::or(column_expr!("a").is_null(), column_expr!("b").is_null()),
        "a IS NULL OR b IS NULL"
    )]
    fn junction_lowers_to_folded_binary_expr(#[case] kernel: Pred, #[case] expected: &str) {
        assert_eq!(lower(kernel), expected);
    }

    #[test]
    fn multi_element_and_folds_left_associatively() {
        let kernel = Pred::and_from([
            column_expr!("a").is_null(),
            column_expr!("b").is_null(),
            column_expr!("c").is_null(),
        ]);
        assert_eq!(lower(kernel), "a IS NULL AND b IS NULL AND c IS NULL");
    }

    #[test]
    fn boolean_expression_delegates_to_expression_converter() {
        assert_eq!(lower(Pred::from_expr(column_expr!("a"))), "a");
    }

    #[test]
    fn unknown_predicate_is_unsupported() {
        to_df_predicate(&Pred::Unknown("mystery".into()), &test_schema()).unwrap_err();
    }
}
