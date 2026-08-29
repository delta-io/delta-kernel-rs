//! Conversion from a kernel [`Predicate`](KernelPredicate) to a boolean-valued DataFusion
//! [`Expr`](DFExpr).

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::common::DFSchema;
use datafusion::functions_nested::expr_fn::{array_compact, array_empty, array_has, cardinality};
use datafusion::logical_expr::utils::{conjunction, disjunction};
use datafusion::logical_expr::{binary_expr, lit, Expr as DFExpr, ExprSchemable, Operator};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::expressions::{
    BinaryPredicate as KernelBinaryPredicate, BinaryPredicateOp as KernelBinaryPredicateOp,
    Expression as KernelExpression, JunctionPredicate as KernelJunctionPredicate,
    JunctionPredicateOp as KernelJunctionPredicateOp, Predicate as KernelPredicate,
    UnaryPredicate as KernelUnaryPredicate, UnaryPredicateOp as KernelUnaryPredicateOp,
};
use delta_kernel::schema::StructType;
use delta_kernel::{DeltaResult, Error};

use crate::expression::to_df_expr;

/// Converts a kernel [`Predicate`](KernelPredicate) into a boolean-valued DataFusion
/// [`Expr`](DFExpr), checking column references against `input_schema`.
///
/// # Errors
/// Returns [`Error::unsupported`] for engine-defined (`Opaque`) or `Unknown` predicates. Also
/// propagates errors from child expressions, such as an unresolved column or an interval literal
/// that has no Arrow equivalent.
pub fn to_df_predicate_expr(
    pred: &KernelPredicate,
    input_schema: &StructType,
) -> DeltaResult<DFExpr> {
    match pred {
        KernelPredicate::BooleanExpression(expr) => {
            boolean_expression_to_df_predicate_expr(expr, input_schema)
        }
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

/// Lowers an expression used as a predicate and verifies that it resolves to Boolean.
fn boolean_expression_to_df_predicate_expr(
    expression: &KernelExpression,
    input_schema: &StructType,
) -> DeltaResult<DFExpr> {
    let expression = to_df_expr(expression, input_schema, None)?;
    let arrow_schema: ArrowSchema = input_schema.try_into_arrow()?;
    let df_schema = DFSchema::try_from(arrow_schema).map_err(Error::generic_err)?;
    if expression
        .get_type(&df_schema)
        .map_err(Error::generic_err)?
        != datafusion::arrow::datatypes::DataType::Boolean
    {
        return Err(Error::invalid_expression(
            "an expression used as a predicate must resolve to Boolean",
        ));
    }
    Ok(expression)
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

/// Lowers Kernel's array-valued `IN` to DataFusion array functions. This accepts any left
/// expression and any array-valued right expression, while preserving SQL three-valued null
/// semantics.
///
/// # Errors
/// Returns an error if either operand cannot be lowered. DataFusion rejects a non-array right
/// operand or operands without a common comparison type while planning the enclosing expression.
fn in_to_df_predicate_expr(
    value: &KernelExpression,
    list: &KernelExpression,
    input_schema: &StructType,
) -> DeltaResult<DFExpr> {
    let value = to_df_expr(value, input_schema, None)?;
    let list = to_df_expr(list, input_schema, None)?;
    let membership = array_has(list.clone(), value.clone());
    let arrow_schema: ArrowSchema = input_schema.try_into_arrow()?;
    let df_schema = DFSchema::try_from(arrow_schema).map_err(Error::generic_err)?;
    let list_type = list.get_type(&df_schema).map_err(Error::generic_err)?;
    let element_nullable = match list_type {
        datafusion::arrow::datatypes::DataType::List(field)
        | datafusion::arrow::datatypes::DataType::LargeList(field) => field.is_nullable(),
        datafusion::arrow::datatypes::DataType::FixedSizeList(field, _) => field.is_nullable(),
        _ => true,
    };
    let may_return_null = value.nullable(&df_schema).map_err(Error::generic_err)?
        || list.nullable(&df_schema).map_err(Error::generic_err)?
        || element_nullable;
    if !may_return_null {
        return Ok(membership.is_true());
    }

    let list_has_null = cardinality(list.clone()).gt(cardinality(array_compact(list.clone())));
    let null = lit(datafusion::common::ScalarValue::Boolean(None));
    Ok(DFExpr::Case(datafusion::logical_expr::expr::Case::new(
        None,
        vec![
            (Box::new(membership.is_true()), Box::new(lit(true))),
            (Box::new(list.clone().is_null()), Box::new(null.clone())),
            (Box::new(array_empty(list.clone())), Box::new(lit(false))),
            (Box::new(value.is_null()), Box::new(null.clone())),
            (Box::new(list_has_null), Box::new(null)),
        ],
        Some(Box::new(lit(false))),
    )))
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

    use datafusion::arrow::array::{
        new_null_array, Array, AsArray, Int64Array, ListArray, RecordBatch,
    };
    use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use datafusion::arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
    use datafusion::common::DFSchema;
    use datafusion::prelude::SessionContext;
    use delta_kernel::engine::arrow_conversion::TryIntoArrow;
    use delta_kernel::expressions::{
        col, lit, null_lit, ArrayData as KernelArrayData, Expression as KernelExpr,
        Predicate as KernelPred, Scalar as KernelScalar,
    };
    use delta_kernel::schema::{schema, ArrayType, DataType};
    use rstest::rstest;

    use super::*;

    // === Shared helpers ===

    /// Columns these tests resolve against.
    fn test_schema() -> StructType {
        schema! {
            nullable "a": LONG,
            nullable "b": LONG,
            nullable "c": LONG,
            nullable "list": [ nullable LONG ],
            nullable "flag": BOOLEAN,
        }
    }

    /// Lowers a predicate and returns its DataFusion `Display` string.
    fn lower(pred: KernelPred) -> String {
        to_df_predicate_expr(&pred, &test_schema())
            .unwrap()
            .to_string()
    }

    /// Lowers a predicate and runs it over one all-null row.
    fn evaluate(pred: KernelPred) -> Option<bool> {
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
        (!result.is_null(0)).then(|| result.value(0))
    }

    /// A literal `Scalar::Array` of longs.
    fn long_array(values: impl IntoIterator<Item = i64>) -> KernelExpr {
        let elements: Vec<KernelScalar> = values.into_iter().map(KernelScalar::Long).collect();
        let array =
            KernelArrayData::try_new(ArrayType::new(DataType::LONG, false), elements).unwrap();
        lit(KernelScalar::Array(array))
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
        lit(KernelScalar::Array(array))
    }

    // === Tests ===

    #[rstest]
    // Primitive comparisons lower to a native binary op.
    #[case::eq(col!("a").eq(lit(1i64)), "a = Int64(1)")]
    #[case::lt(col!("a").lt(lit(1i64)), "a < Int64(1)")]
    #[case::gt(col!("a").gt(lit(1i64)), "a > Int64(1)")]
    #[case::distinct(
        col!("a").distinct(lit(1i64)),
        "a IS DISTINCT FROM Int64(1)"
    )]
    // Kernel has no <=/>=/!= ops: each is `Not` of a comparison, so it renders negated.
    #[case::ne(col!("a").ne(lit(1i64)), "NOT a = Int64(1)")]
    #[case::le(col!("a").le(lit(1i64)), "NOT a > Int64(1)")]
    #[case::ge(col!("a").ge(lit(1i64)), "NOT a < Int64(1)")]
    // Unary.
    #[case::is_null(col!("a").is_null(), "a IS NULL")]
    #[case::is_not_null(col!("a").is_not_null(), "NOT a IS NULL")]
    // Junctions fold left-associatively.
    #[case::and(
        KernelPred::and(col!("a").is_null(), col!("b").is_null()),
        "a IS NULL AND b IS NULL"
    )]
    #[case::or(
        KernelPred::or(col!("a").is_null(), col!("b").is_null()),
        "a IS NULL OR b IS NULL"
    )]
    #[case::multi_and(
        KernelPred::and_from([
            col!("a").is_null(),
            col!("b").is_null(),
            col!("c").is_null(),
        ]),
        "a IS NULL AND b IS NULL AND c IS NULL"
    )]
    // A bare boolean expression goes straight to the expression converter.
    #[case::boolean_expression(KernelPred::from_expr(col!("flag")), "flag")]
    // Nested predicates. DataFusion's Display omits parens around the junction under a `Not`, but
    // the `Expr` tree is still `Not(And(..))`.
    #[case::not_of_junction(
        KernelPred::not(KernelPred::and(col!("a").is_null(), col!("b").is_null())),
        "NOT a IS NULL AND b IS NULL"
    )]
    #[case::junction_of_junction(
        KernelPred::or(
            KernelPred::and(col!("a").is_null(), col!("b").is_null()),
            col!("c").is_null(),
        ),
        "a IS NULL AND b IS NULL OR c IS NULL"
    )]
    #[case::and_of_comparisons(
        KernelPred::and(
            col!("a").eq(lit(1i64)),
            col!("b").gt(lit(2i64)),
        ),
        "a = Int64(1) AND b > Int64(2)"
    )]
    fn predicate_lowers_to_expected(#[case] kernel: KernelPred, #[case] expected: &str) {
        assert_eq!(lower(kernel), expected);
    }

    #[rstest]
    // Engine-defined and unknown predicates have no DataFusion equivalent.
    #[case::unknown(KernelPred::Unknown("mystery".into()))]
    #[case::non_boolean_expression(KernelPred::from_expr(col!("a")))]
    fn unsupported_predicate_is_an_error(#[case] pred: KernelPred) {
        to_df_predicate_expr(&pred, &test_schema()).unwrap_err();
    }

    #[test]
    fn in_accepts_expression_operands() {
        for pred in [
            KernelPred::binary(KernelBinaryPredicateOp::In, col!("a"), long_array([1, 2])),
            KernelPred::binary(KernelBinaryPredicateOp::In, col!("a"), col!("list")),
        ] {
            to_df_predicate_expr(&pred, &test_schema()).unwrap();
        }
    }

    #[test]
    fn in_rejects_non_array_right_operand_during_datafusion_planning() {
        let pred = KernelPred::binary(KernelBinaryPredicateOp::In, lit(1i64), col!("b"));
        let expression = to_df_predicate_expr(&pred, &test_schema()).unwrap();
        let arrow_schema: ArrowSchema = (&test_schema()).try_into_arrow().unwrap();
        let df_schema = DFSchema::try_from(arrow_schema).unwrap();
        SessionContext::new()
            .create_physical_expr(expression, &df_schema)
            .unwrap_err();
    }

    /// `IN` follows SQL three-valued logic, and `NOT IN` negates the resulting Boolean or null.
    #[rstest]
    #[case::present(Some(2), &[Some(1), Some(2)], Some(true))]
    #[case::absent(Some(9), &[Some(1), Some(2)], Some(false))]
    #[case::null_value_without_null_element(None, &[Some(1), Some(2)], None)]
    #[case::null_value_with_null_element(None, &[Some(1), None], None)]
    #[case::present_alongside_null_element(Some(1), &[Some(1), None], Some(true))]
    #[case::absent_alongside_null_element(Some(9), &[Some(1), None], None)]
    #[case::only_null_element(None, &[None], None)]
    fn in_predicate_uses_three_valued_logic(
        #[case] value: Option<i64>,
        #[case] elements: &[Option<i64>],
        #[case] expected: Option<bool>,
    ) {
        let in_pred = KernelPred::binary(
            KernelBinaryPredicateOp::In,
            match value {
                Some(n) => lit(n),
                None => null_lit(DataType::LONG),
            },
            nullable_long_array(elements.to_vec()),
        );

        assert_eq!(evaluate(in_pred.clone()), expected);
        assert_eq!(
            evaluate(KernelPred::not(in_pred)),
            expected.map(|value| !value)
        );
    }

    /// An array-valued column follows the same SQL null behavior as a literal array.
    #[rstest]
    #[case::present(Some(2), &[Some(1), Some(2)], Some(true))]
    #[case::absent(Some(9), &[Some(1), Some(2)], Some(false))]
    #[case::null_value_without_null_element(None, &[Some(1), Some(2)], None)]
    #[case::null_value_with_null_element(None, &[Some(1), None], None)]
    #[case::present_alongside_null_element(Some(1), &[Some(1), None], Some(true))]
    #[case::absent_alongside_null_element(Some(9), &[Some(1), None], None)]
    #[case::only_null_element(None, &[None], None)]
    fn in_list_column_uses_three_valued_logic(
        #[case] value: Option<i64>,
        #[case] elements: &[Option<i64>],
        #[case] expected: Option<bool>,
    ) {
        let in_pred = KernelPred::binary(
            KernelBinaryPredicateOp::In,
            match value {
                Some(n) => lit(n),
                None => null_lit(DataType::LONG),
            },
            col!("list"),
        );
        // Column `list` carries the elements; `a`/`b`/`c` are unused.
        let batch = list_column_batch(elements);

        assert_eq!(evaluate_over(in_pred.clone(), &batch), expected);
        assert_eq!(
            evaluate_over(KernelPred::not(in_pred), &batch),
            expected.map(|value| !value)
        );
    }

    /// Lowers `pred` against [`test_schema`] and evaluates it over the single-row `batch`.
    fn evaluate_over(pred: KernelPred, batch: &RecordBatch) -> Option<bool> {
        let df_expr = to_df_predicate_expr(&pred, &test_schema()).unwrap();
        let df_schema = DFSchema::try_from(batch.schema()).unwrap();
        let result = SessionContext::new()
            .create_physical_expr(df_expr, &df_schema)
            .unwrap()
            .evaluate(batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        let result = result.as_boolean();

        assert_eq!(result.len(), 1, "expected a single-row result");
        (!result.is_null(0)).then(|| result.value(0))
    }

    /// A one-row batch whose single `list` column holds `elements`.
    fn list_column_batch(elements: &[Option<i64>]) -> RecordBatch {
        // Reuse the schema's own `list` field so the built array matches the name and metadata
        // kernel synthesizes (an `element` child field, possibly with field-id metadata).
        let arrow_schema: ArrowSchema = (&test_schema()).try_into_arrow().unwrap();
        let list_field = arrow_schema.field_with_name("list").unwrap().clone();
        let ArrowDataType::List(element_field) = list_field.data_type().clone() else {
            unreachable!("`list` is declared as an array type");
        };
        let list = ListArray::new(
            element_field,
            OffsetBuffer::new(ScalarBuffer::from(vec![0i32, elements.len() as i32])),
            Arc::new(Int64Array::from(elements.to_vec())),
            None,
        );
        RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![list_field])),
            vec![Arc::new(list)],
        )
        .unwrap()
    }
}
