//! Narrow expression-type inference for cursor schema derivation.
//!
//! Used by the SSA cursor builder ([`super::ssa`] consumers) to derive a [`DataType`] for
//! each named expression in a `Project` so the cursor knows the output schema without the
//! caller spelling it out. Coverage is intentionally minimal -- the variants exercised by
//! the FSR / Scan / CDF builders today (`Literal`, `Column`, `Predicate`, `Variadic::Array`,
//! `Variadic::Coalesce`, `If`, `Unary::ToJson`, `ParseJson`, plus boolean predicates).
//!
//! Hard variants (`Struct`, `Transform`, `Binary`, `MapToStruct`, `Opaque`, `Unknown`) return
//! a [`DeltaErrorCode::DeltaCommandInvariantViolation`] error directing the caller to use
//! `project_with_schema`, which takes an explicit output schema and skips inference. See
//! `RENAME_PROPOSAL.md` for the design rationale (option "B-narrow").
//!
//! Inference does NOT validate the expression -- it assumes well-formedness (e.g. `Coalesce`
//! arms produce compatible types). The kernel evaluator validates at runtime.

use crate::delta_error;
use crate::expressions::{Expression, UnaryExpressionOp, VariadicExpressionOp};
use crate::plans::errors::{DeltaError, DeltaErrorCode, DeltaResultExt};
use crate::schema::{ArrayType, DataType, StructType};

/// Infer `expr`'s output [`DataType`] when evaluated against rows of `input_schema`.
///
/// Returns an error for variants outside the narrow supported set; see module docs.
pub(crate) fn infer_expression_type(
    expr: &Expression,
    input_schema: &StructType,
) -> Result<DataType, DeltaError> {
    match expr {
        Expression::Literal(scalar) => Ok(scalar.data_type()),

        Expression::Column(col) => {
            let fields = input_schema
                .walk_column_fields(col)
                .or_delta(DeltaErrorCode::DeltaCommandInvariantViolation)?;
            // walk_column_fields guarantees a non-empty result on success.
            let leaf = fields.last().ok_or_else(|| {
                delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    "walk_column_fields returned empty path for non-empty column",
                )
            })?;
            Ok(leaf.data_type().clone())
        }

        Expression::Predicate(_) => Ok(DataType::BOOLEAN),

        Expression::Unary(u) => match u.op {
            UnaryExpressionOp::ToJson => Ok(DataType::STRING),
        },

        Expression::Variadic(v) => match v.op {
            VariadicExpressionOp::Coalesce => unify_arms(
                v.exprs
                    .iter()
                    .map(|e| infer_expression_type(e, input_schema)),
                "coalesce",
            ),
            VariadicExpressionOp::Array => {
                let elem = unify_arms(
                    v.exprs
                        .iter()
                        .map(|e| infer_expression_type(e, input_schema)),
                    "array",
                )?;
                Ok(DataType::Array(Box::new(ArrayType::new(elem, true))))
            }
        },

        Expression::If(if_expr) => unify_arms(
            [
                infer_expression_type(&if_expr.then_expr, input_schema),
                infer_expression_type(&if_expr.else_expr, input_schema),
            ]
            .into_iter(),
            "if",
        ),

        Expression::ParseJson(p) => Ok(DataType::Struct(Box::new((*p.output_schema).clone()))),

        // Hard variants -- caller should use project_with_schema instead.
        Expression::Struct(..)
        | Expression::Transform(_)
        | Expression::Binary(_)
        | Expression::MapToStruct(_)
        | Expression::Opaque(_)
        | Expression::Unknown(_) => Err(delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            "expression variant cannot be inferred by the cursor builder; use \
             project_with_schema with an explicit output schema",
        )),
    }
}

/// Unify the inferred types of N expression arms into a single output type.
///
/// All arms must share the same [`DataType`] (no implicit promotion). Empty input
/// is rejected -- the operator guarantees at least one arm at construction.
fn unify_arms(
    types: impl Iterator<Item = Result<DataType, DeltaError>>,
    op: &str,
) -> Result<DataType, DeltaError> {
    let mut iter = types.peekable();
    let first = match iter.next() {
        Some(Ok(t)) => t,
        Some(Err(e)) => return Err(e),
        None => {
            return Err(delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "{op}: cannot infer type with zero arms",
            ));
        }
    };
    for next in iter {
        let ty = next?;
        if ty != first {
            return Err(delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "{op}: arm types disagree -- got {first:?} and {ty:?}",
            ));
        }
    }
    Ok(first)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{
        col, ColumnName, Expression, IfExpression, Predicate, Scalar, UnaryExpression,
        VariadicExpression,
    };
    use crate::schema::{StructField, StructType};

    fn input_schema() -> StructType {
        StructType::try_new(vec![
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("ts", DataType::LONG),
            StructField::nullable("flag", DataType::BOOLEAN),
        ])
        .unwrap()
    }

    #[test]
    fn literal_returns_scalar_type() {
        let s = input_schema();
        assert_eq!(
            infer_expression_type(&Expression::literal(42i64), &s).unwrap(),
            DataType::LONG,
        );
        assert_eq!(
            infer_expression_type(&Expression::literal(Scalar::Null(DataType::STRING)), &s)
                .unwrap(),
            DataType::STRING,
        );
    }

    #[test]
    fn column_returns_field_type() {
        let s = input_schema();
        assert_eq!(
            infer_expression_type(&col("id"), &s).unwrap(),
            DataType::STRING,
        );
        assert_eq!(
            infer_expression_type(&col("ts"), &s).unwrap(),
            DataType::LONG,
        );
    }

    #[test]
    fn unknown_column_errors() {
        let s = input_schema();
        let expr = Expression::Column(ColumnName::new(["missing"]));
        let err = infer_expression_type(&expr, &s).unwrap_err();
        assert!(
            err.to_string().contains("missing"),
            "expected missing-column error, got: {err}",
        );
    }

    #[test]
    fn predicate_is_boolean() {
        let s = input_schema();
        let expr = Expression::Predicate(Box::new(Predicate::BooleanExpression(col("flag"))));
        assert_eq!(infer_expression_type(&expr, &s).unwrap(), DataType::BOOLEAN,);
    }

    #[test]
    fn to_json_is_string() {
        let s = input_schema();
        let expr = Expression::Unary(UnaryExpression {
            op: UnaryExpressionOp::ToJson,
            expr: Box::new(col("id")),
        });
        assert_eq!(infer_expression_type(&expr, &s).unwrap(), DataType::STRING);
    }

    #[test]
    fn coalesce_unifies_matching_arms() {
        let s = input_schema();
        let expr = Expression::Variadic(VariadicExpression {
            op: VariadicExpressionOp::Coalesce,
            exprs: vec![col("id"), Expression::literal("default")],
        });
        assert_eq!(infer_expression_type(&expr, &s).unwrap(), DataType::STRING);
    }

    #[test]
    fn coalesce_with_disagreeing_arms_errors() {
        let s = input_schema();
        let expr = Expression::Variadic(VariadicExpression {
            op: VariadicExpressionOp::Coalesce,
            exprs: vec![col("id"), col("ts")],
        });
        let err = infer_expression_type(&expr, &s).unwrap_err();
        assert!(
            err.to_string().contains("disagree"),
            "expected disagreement error, got: {err}",
        );
    }

    #[test]
    fn array_wraps_unified_element_type() {
        let s = input_schema();
        let expr = Expression::Variadic(VariadicExpression {
            op: VariadicExpressionOp::Array,
            exprs: vec![col("id"), Expression::literal("x")],
        });
        let ty = infer_expression_type(&expr, &s).unwrap();
        match ty {
            DataType::Array(arr) => {
                assert_eq!(arr.element_type, DataType::STRING);
                assert!(arr.contains_null);
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    #[test]
    fn if_unifies_then_else() {
        let s = input_schema();
        let expr = Expression::If(IfExpression {
            condition: Box::new(Predicate::BooleanExpression(col("flag"))),
            then_expr: Box::new(col("ts")),
            else_expr: Box::new(Expression::literal(0i64)),
        });
        assert_eq!(infer_expression_type(&expr, &s).unwrap(), DataType::LONG);
    }

    #[test]
    fn hard_variants_error_with_actionable_message() {
        let s = input_schema();
        let expr = Expression::Unknown("opaque".to_string());
        let err = infer_expression_type(&expr, &s).unwrap_err();
        assert!(
            err.to_string().contains("project_with_schema"),
            "expected guidance to use project_with_schema, got: {err}",
        );
    }
}
