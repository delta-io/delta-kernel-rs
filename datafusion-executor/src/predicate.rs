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
/// [`Expr`](DFExpr), checking column references against `input_schema`.
///
/// # Errors
/// Returns [`Error::unsupported`] for engine-defined (`Opaque`) or `Unknown` predicates, and for an
/// `IN` whose right side is not a literal array. Also propagates errors from child expressions,
/// such as an unresolved column or an interval literal (which has no Arrow equivalent).
pub fn to_df_predicate_expr(
    pred: &KernelPredicate,
    input_schema: &StructType,
) -> DeltaResult<DFExpr> {
    match pred {
        KernelPredicate::BooleanExpression(expr) => to_df_expr(expr, input_schema, None),
        KernelPredicate::Not(inner) => {
            let df_inner = to_df_predicate_expr(inner, input_schema)?;
            Ok(DFExpr::Not(Box::new(df_inner)))
        }
        KernelPredicate::Unary(unary) => unary_to_df_predicate_expr(unary, input_schema),
        KernelPredicate::Binary(binary) => binary_to_df_predicate_expr(binary, input_schema),
        KernelPredicate::Junction(junction) => {
            junction_to_df_predicate_expr(junction, input_schema)
        }
        KernelPredicate::Opaque(_) => Err(Error::unsupported(
            "cannot convert an engine-defined Opaque predicate",
        )),
        KernelPredicate::Unknown(name) => Err(Error::unsupported(format!(
            "cannot convert Unknown predicate {name:?}"
        ))),
    }
}

/// Lowers a unary predicate.
fn unary_to_df_predicate_expr(
    unary: &KernelUnaryPredicate,
    input_schema: &StructType,
) -> DeltaResult<DFExpr> {
    let expr = to_df_expr(&unary.expr, input_schema, None)?;
    match unary.op {
        KernelUnaryPredicateOp::IsNull => Ok(DFExpr::IsNull(Box::new(expr))),
    }
}

/// Lowers a binary predicate.
fn binary_to_df_predicate_expr(
    binary: &KernelBinaryPredicate,
    input_schema: &StructType,
) -> DeltaResult<DFExpr> {
    let op = match binary.op {
        KernelBinaryPredicateOp::In => {
            return in_to_df_predicate_expr(&binary.left, &binary.right, input_schema)
        }
        KernelBinaryPredicateOp::Equal => Operator::Eq,
        KernelBinaryPredicateOp::LessThan => Operator::Lt,
        KernelBinaryPredicateOp::GreaterThan => Operator::Gt,
        KernelBinaryPredicateOp::Distinct => Operator::IsDistinctFrom,
    };
    let left = to_df_expr(&binary.left, input_schema, None)?;
    let right = to_df_expr(&binary.right, input_schema, None)?;
    Ok(binary_expr(left, op, right))
}

/// Lowers an `IN` predicate. Kernel models `x IN (..)` as `Binary(In, value, literal_array)`;
/// DataFusion uses [`InList`], whose trailing flag negates the test. Kernel has no negated `IN`
/// (`NOT IN` arrives as `Not(Binary(In, ..))`, handled by the caller's `Not` arm), so the flag is
/// always `false` here.
///
/// The two differ on nulls. DataFusion uses SQL three-valued logic, so a null on either side makes
/// the result null: `NULL IN (1, 2)` and `2 IN (1, NULL)` are both null. Kernel compares scalars
/// structurally and always answers true or false, so a null value matches a null element:
/// `NULL IN (1, NULL)` is true and `NULL IN (1, 2)` is false.
///
/// To match kernel, this lowering:
/// - Drops null elements from the list, adding `IS NULL` on the value if there was one.
/// - Wraps the membership test in `IS TRUE`, turning DataFusion's null into false.
///
/// Both go inside the predicate, not around it, because the caller's `Not` arm may negate the
/// result and a leftover null would change which rows a filter keeps. Kernel reads
/// `NOT NULL IN (1, 2)` as `NOT false` = true, but an unguarded DataFusion `NOT NULL` stays null
/// and drops the row.
///
/// KNOWN DIVERGENCES from kernel's Arrow evaluator, which only handles a literal value against a
/// literal array or against a list column:
/// - A column value against a literal array (plain `x IN (1, 2)`) errors in kernel. This lowering
///   answers it, using the same structural rule.
/// - A list column never reaches here, since a non-literal right side is rejected below. Kernel
///   evaluates that shape with arrow's `in_list` (via `prim_array_cmp!` in `engine::arrow_utils`),
///   which drops the null buffer and skips null elements, so a null value answers false instead of
///   matching a null element. It also panics when the value's type differs from the element type,
///   since the macro picks the type from the value alone.
///
/// # Errors
/// Returns [`Error::unsupported`] if the right operand is not a literal array.
fn in_to_df_predicate_expr(
    value: &KernelExpression,
    list: &KernelExpression,
    input_schema: &StructType,
) -> DeltaResult<DFExpr> {
    let KernelExpression::Literal(KernelScalar::Array(array)) = list else {
        return Err(Error::unsupported(
            "converting an IN predicate requires a literal array on the right-hand side",
        ));
    };
    let (null_elements, elements): (Vec<_>, Vec<_>) =
        array.array_elements().iter().partition(|s| s.is_null());
    let elements: DeltaResult<Vec<DFExpr>> = elements
        .iter()
        .map(|scalar| Ok(lit(to_df_scalar(scalar)?)))
        .collect();

    let value = to_df_expr(value, input_schema, None)?;
    let in_list = InList::new(Box::new(value.clone()), elements?, false);
    let is_member = DFExpr::InList(in_list).is_true();

    match null_elements.is_empty() {
        true => Ok(is_member),
        false => Ok(is_member.or(value.is_null())),
    }
}

/// Lowers a junction (`And`/`Or`) by converting each child and combining them with DataFusion's
/// left-associative [`conjunction`]/[`disjunction`] helpers.
fn junction_to_df_predicate_expr(
    junction: &KernelJunctionPredicate,
    input_schema: &StructType,
) -> DeltaResult<DFExpr> {
    let preds: DeltaResult<Vec<DFExpr>> = junction
        .preds
        .iter()
        .map(|pred| to_df_predicate_expr(pred, input_schema))
        .collect();
    // An empty junction lowers `AND` to `true` and `OR` to `false`, keeping kernel semantics.
    match junction.op {
        KernelJunctionPredicateOp::And => Ok(conjunction(preds?).unwrap_or_else(|| lit(true))),
        KernelJunctionPredicateOp::Or => Ok(disjunction(preds?).unwrap_or_else(|| lit(false))),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{new_null_array, Array, AsArray, RecordBatch};
    use datafusion::arrow::datatypes::Schema as ArrowSchema;
    use datafusion::common::DFSchema;
    use datafusion::prelude::SessionContext;
    use delta_kernel::engine::arrow_conversion::TryIntoArrow;
    use delta_kernel::expressions::{
        column_expr, ArrayData as KernelArrayData, Expression as KernelExpr,
        Predicate as KernelPred,
    };
    use delta_kernel::schema::{ArrayType, DataType, StructField};
    use rstest::rstest;

    use super::*;

    // === Shared helpers ===

    /// Columns these tests resolve against: top-level `a`, `b`, `c`, all `long`.
    fn test_schema() -> StructType {
        StructType::try_new([
            StructField::nullable("a", DataType::LONG),
            StructField::nullable("b", DataType::LONG),
            StructField::nullable("c", DataType::LONG),
        ])
        .unwrap()
    }

    /// Lowers a predicate and returns its DataFusion `Display` string.
    fn lower(pred: KernelPred) -> String {
        to_df_predicate_expr(&pred, &test_schema())
            .unwrap()
            .to_string()
    }

    /// Lowers a predicate, runs it over one all-null row, and asserts the result is not null.
    fn evaluate(pred: KernelPred) -> bool {
        let df_expr = to_df_predicate_expr(&pred, &test_schema()).unwrap();
        let arrow_schema: ArrowSchema = (&test_schema()).try_into_arrow().unwrap();
        let arrow_schema = Arc::new(arrow_schema);
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            arrow_schema
                .fields()
                .iter()
                .map(|field| new_null_array(field.data_type(), 1))
                .collect(),
        )
        .unwrap();

        let df_schema = DFSchema::try_from(arrow_schema).unwrap();
        let physical = SessionContext::new()
            .create_physical_expr(df_expr, &df_schema)
            .unwrap();
        let result = physical
            .evaluate(&batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        let result = result.as_boolean();

        assert_eq!(result.null_count(), 0, "predicate evaluated to null");
        result.value(0)
    }

    /// A literal `Scalar::Array` of longs.
    fn long_array(values: impl IntoIterator<Item = i64>) -> KernelExpr {
        let elements: Vec<KernelScalar> = values.into_iter().map(KernelScalar::Long).collect();
        let array =
            KernelArrayData::try_new(ArrayType::new(DataType::LONG, false), elements).unwrap();
        KernelExpr::literal(KernelScalar::Array(array))
    }

    /// A literal `Scalar::Array` of longs where `None` becomes a null element.
    fn nullable_long_array(values: impl IntoIterator<Item = Option<i64>>) -> KernelExpr {
        let elements: Vec<KernelScalar> = values
            .into_iter()
            .map(|v| match v {
                Some(n) => KernelScalar::Long(n),
                None => KernelScalar::Null(DataType::LONG),
            })
            .collect();
        let array =
            KernelArrayData::try_new(ArrayType::new(DataType::LONG, true), elements).unwrap();
        KernelExpr::literal(KernelScalar::Array(array))
    }

    // === Tests ===

    #[rstest]
    // Primitive comparisons lower to a native binary op.
    #[case::eq(column_expr!("a").eq(KernelExpr::literal(1i64)), "a = Int64(1)")]
    #[case::lt(column_expr!("a").lt(KernelExpr::literal(1i64)), "a < Int64(1)")]
    #[case::gt(column_expr!("a").gt(KernelExpr::literal(1i64)), "a > Int64(1)")]
    #[case::distinct(
        column_expr!("a").distinct(KernelExpr::literal(1i64)),
        "a IS DISTINCT FROM Int64(1)"
    )]
    // Kernel has no <=/>=/!= ops: each is `Not` of a comparison, so it renders negated.
    #[case::ne(column_expr!("a").ne(KernelExpr::literal(1i64)), "NOT a = Int64(1)")]
    #[case::le(column_expr!("a").le(KernelExpr::literal(1i64)), "NOT a > Int64(1)")]
    #[case::ge(column_expr!("a").ge(KernelExpr::literal(1i64)), "NOT a < Int64(1)")]
    // Unary.
    #[case::is_null(column_expr!("a").is_null(), "a IS NULL")]
    #[case::is_not_null(column_expr!("a").is_not_null(), "NOT a IS NULL")]
    // IN / NOT IN. `IS TRUE` turns DataFusion's null into kernel's false, and sits inside the `Not`
    // so a null value gives `NOT false` instead of `NOT NULL`.
    #[case::in_list(
        KernelPred::binary(KernelBinaryPredicateOp::In, column_expr!("a"), long_array([1, 2, 3])),
        "a IN ([Int64(1), Int64(2), Int64(3)]) IS TRUE"
    )]
    #[case::not_in(
        KernelPred::not(KernelPred::binary(KernelBinaryPredicateOp::In, column_expr!("a"), long_array([1, 2]))),
        "NOT a IN ([Int64(1), Int64(2)]) IS TRUE"
    )]
    #[case::null_needle_in_list(
        KernelPred::binary(
            KernelBinaryPredicateOp::In,
            KernelExpr::null_literal(DataType::LONG),
            long_array([1, 2]),
        ),
        "Int64(NULL) IN ([Int64(1), Int64(2)]) IS TRUE"
    )]
    // A null element leaves the list and becomes an `IS NULL` check, so a null value still matches
    // it the way kernel does.
    #[case::null_element_in_list(
        KernelPred::binary(
            KernelBinaryPredicateOp::In,
            column_expr!("a"),
            nullable_long_array([Some(1), None]),
        ),
        "a IN ([Int64(1)]) IS TRUE OR a IS NULL"
    )]
    // Junctions fold left-associatively.
    #[case::and(
        KernelPred::and(column_expr!("a").is_null(), column_expr!("b").is_null()),
        "a IS NULL AND b IS NULL"
    )]
    #[case::or(
        KernelPred::or(column_expr!("a").is_null(), column_expr!("b").is_null()),
        "a IS NULL OR b IS NULL"
    )]
    #[case::multi_and(
        KernelPred::and_from([
            column_expr!("a").is_null(),
            column_expr!("b").is_null(),
            column_expr!("c").is_null(),
        ]),
        "a IS NULL AND b IS NULL AND c IS NULL"
    )]
    // A bare boolean expression goes straight to the expression converter.
    #[case::boolean_expression(KernelPred::from_expr(column_expr!("a")), "a")]
    // Nested predicates. DataFusion's Display omits parens around the junction under a `Not`, but
    // the `Expr` tree is still `Not(And(..))`.
    #[case::not_of_junction(
        KernelPred::not(KernelPred::and(column_expr!("a").is_null(), column_expr!("b").is_null())),
        "NOT a IS NULL AND b IS NULL"
    )]
    #[case::junction_of_junction(
        KernelPred::or(
            KernelPred::and(column_expr!("a").is_null(), column_expr!("b").is_null()),
            column_expr!("c").is_null(),
        ),
        "a IS NULL AND b IS NULL OR c IS NULL"
    )]
    #[case::and_of_comparisons(
        KernelPred::and(
            column_expr!("a").eq(KernelExpr::literal(1i64)),
            column_expr!("b").gt(KernelExpr::literal(2i64)),
        ),
        "a = Int64(1) AND b > Int64(2)"
    )]
    fn predicate_lowers_to_expected(#[case] kernel: KernelPred, #[case] expected: &str) {
        assert_eq!(lower(kernel), expected);
    }

    #[rstest]
    // Engine-defined and unknown predicates have no DataFusion equivalent.
    #[case::unknown(KernelPred::Unknown("mystery".into()))]
    // An `IN` whose right side is not a literal array cannot be lowered.
    #[case::in_without_literal_array(
        KernelPred::binary(KernelBinaryPredicateOp::In, column_expr!("a"), column_expr!("b"))
    )]
    fn unsupported_predicate_is_an_error(#[case] pred: KernelPred) {
        to_df_predicate_expr(&pred, &test_schema()).unwrap_err();
    }

    /// `IN` answers true or false but never null, so it still works under a `NOT`. A null value
    /// matches a null element, following kernel's structural comparison.
    #[rstest]
    #[case::present(Some(2), &[Some(1), Some(2)], true)]
    #[case::absent(Some(9), &[Some(1), Some(2)], false)]
    #[case::null_needle_without_null_element(None, &[Some(1), Some(2)], false)]
    #[case::null_needle_matches_null_element(None, &[Some(1), None], true)]
    #[case::present_alongside_null_element(Some(1), &[Some(1), None], true)]
    #[case::absent_alongside_null_element(Some(9), &[Some(1), None], false)]
    #[case::only_null_element(None, &[None], true)]
    fn in_predicate_matches_membership_and_never_nulls(
        #[case] needle: Option<i64>,
        #[case] elements: &[Option<i64>],
        #[case] expected: bool,
    ) {
        let in_pred = KernelPred::binary(
            KernelBinaryPredicateOp::In,
            match needle {
                Some(n) => KernelExpr::literal(n),
                None => KernelExpr::null_literal(DataType::LONG),
            },
            nullable_long_array(elements.to_vec()),
        );

        assert_eq!(evaluate(in_pred.clone()), expected);
        assert_eq!(evaluate(KernelPred::not(in_pred)), !expected);
    }
}
