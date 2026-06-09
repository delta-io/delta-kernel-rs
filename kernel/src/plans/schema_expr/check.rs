//! Bidirectional expression type checker for builder schema derivation. Each public helper
//! corresponds 1:1 to a `PlanBuilder` method so schema derivation lives in one named place:
//!
//! - [`check_expression`] -- bidirectional `(expr, expected)` check with strict [`DataType::eq`]
//!   comparison; the primitive behind `field_op` and [`check_select`].
//! - [`infer_projection_schema`] -- inference for `PlanBuilder::project`.
//! - [`check_projection`] -- arity + per-expression well-formedness for
//!   `PlanBuilder::project_with_schema`. Per-expression types are *not* compared to the declared
//!   output schema; the caller's schema is authoritative and the runtime engine validates the
//!   actual values. This is what lets `project_with_schema` carry column-mapping renames without
//!   faking up a "structural" leaf comparator.
//! - [`check_select`] -- identity-projection for `PlanBuilder::select`, strict-checked
//!   field-by-field via [`check_expression`].
//! - [`validate_exprs`] -- well-formedness check for operator-internal expressions that don't
//!   contribute to the output schema (e.g. `PlanBuilder::max_by_version` group-by).
//!
//! Errors return [`SchemaExprResult`] (boxed `dyn Error`) -- this is a generic type checker that
//! knows nothing about Delta. The plan-builder boundary wraps these via
//! [`DeltaResultExt::or_delta`][crate::plans::errors::DeltaResultExt::or_delta].

use std::borrow::Cow;

use itertools::Itertools;

use crate::expressions::{
    ColumnName, Expression, ExpressionRef, Scalar, UnaryExpressionOp, VariadicExpressionOp,
};
use crate::plans::schema_expr::field_op::{arc_struct_or_invariant, identity_named_expr};
use crate::plans::schema_expr::{SchemaExprError, SchemaExprResult};
use crate::schema::{ArrayType, DataType, SchemaRef, StructField, StructType};
use crate::transforms::ExpressionTransform;

/// Shorthand for `SchemaExprError::from(format!(...))`.
macro_rules! type_err {
    ($($arg:tt)*) => {
        $crate::plans::schema_expr::SchemaExprError::from(::std::format!($($arg)*))
    };
}

/// Pure type synthesis: derive `(data_type, nullable)` from `expr` against `input_schema`.
///
/// Per-variant rules: `Literal` is null iff `Scalar::Null`; `Column` inherits the leaf field;
/// `Predicate` and `ParseJson` are conservatively nullable; `ToJson` propagates the operand's
/// nullability; `Coalesce` is non-null iff *any* arm is non-null; `If` ORs the arm nullabilities;
/// `Array` is outer-non-null with `contains_null = true` (a precise OR would collide with the
/// strict equality used to unify `If`/`Coalesce`/`Array` arms).
///
/// Errors on `Struct`, `Transform`, `Binary`, `MapToStruct`, `Opaque`, `Unknown` -- those need a
/// target field; route through [`check_expression`] instead.
fn synthesize(expr: &Expression, input_schema: &StructType) -> SchemaExprResult<(DataType, bool)> {
    Ok(match expr {
        Expression::Literal(s) => (s.data_type(), matches!(s, Scalar::Null(_))),
        Expression::Column(c) => {
            // walk_column_fields guarantees a non-empty result on success.
            let leaf = *input_schema
                .walk_column_fields(c)?
                .last()
                .ok_or_else(|| type_err!("walk_column_fields returned empty path for {c}"))?;
            (leaf.data_type().clone(), leaf.nullable)
        }
        Expression::Predicate(_) => (DataType::BOOLEAN, true),
        Expression::Unary(u) => match u.op {
            UnaryExpressionOp::ToJson => (DataType::STRING, synthesize(&u.expr, input_schema)?.1),
        },
        Expression::Variadic(v) => {
            let arms: Vec<_> = v
                .exprs
                .iter()
                .map(|e| synthesize(e, input_schema))
                .collect::<SchemaExprResult<_>>()?;
            let elem_ty = unify_types(arms.iter().map(|(t, _)| t.clone()), &v.op.to_string())?;
            match v.op {
                VariadicExpressionOp::Coalesce => (elem_ty, arms.iter().all(|(_, n)| *n)),
                VariadicExpressionOp::Array => (ArrayType::new(elem_ty, true).into(), false),
            }
        }
        Expression::ParseJson(p) => ((*p.output_schema).clone().into(), true),
        Expression::Struct(..)
        | Expression::StructPatch(_)
        | Expression::Binary(_)
        | Expression::MapToStruct(_)
        | Expression::Opaque(_)
        | Expression::Unknown(_) => {
            return Err(type_err!(
                "expression variant has no type rule without a target field; \
                 route through check_expression",
            ))
        }
    })
}

/// Bidirectional type check for `expr` against `input_schema`. Returns `expected.clone()` on
/// success -- the caller's declared `name`/`data_type`/`nullable` are authoritative (synthesized
/// nullability is *not* enforced against `expected.nullable`; callers often tighten based on
/// invariants the type system cannot see).
///
/// Leaf-type comparison is strict [`DataType::eq`]: nested struct field names must match. Callers
/// that need to accept renames (e.g. column-mapping in `project_with_schema`) skip this helper
/// and go through [`check_projection`] instead, which only validates arity + well-formedness.
///
/// Bidirectional-only variants:
/// - `MapToStruct`: `expected.data_type()` must be `Struct`; input must produce `Map<String,
///   String>`.
/// - `Struct`: `expected.data_type()` must be `Struct` with matching arity; recurses on fields.
/// - `StructPatch` / `Opaque` / `Unknown`: no type rule -- column refs validated, `expected` trusted.
///
/// All other variants synthesize and compare to `expected.data_type()` via `DataType::eq`.
pub(crate) fn check_expression(
    expr: &Expression,
    input_schema: &StructType,
    expected: &StructField,
) -> SchemaExprResult<StructField> {
    let want = expected.data_type();
    match expr {
        Expression::MapToStruct(m) => {
            if !matches!(want, DataType::Struct(_)) {
                return Err(type_err!("map_to_struct: expected Struct, got {want:?}"));
            }
            match synthesize(&m.map_expr, input_schema)?.0 {
                DataType::Map(m)
                    if m.key_type == DataType::STRING && m.value_type == DataType::STRING => {}
                bad => {
                    return Err(type_err!(
                        "map_to_struct: input must produce Map<String, String>, got {bad:?}",
                    ))
                }
            }
        }
        Expression::Struct(fields, _) => {
            let DataType::Struct(target) = want else {
                return Err(type_err!("struct: expected Struct, got {want:?}"));
            };
            let target_fields: Vec<_> = target.fields().collect();
            if fields.len() != target_fields.len() {
                return Err(type_err!(
                    "struct: arity mismatch -- {got} expressions, {want_n} expected fields",
                    got = fields.len(),
                    want_n = target_fields.len(),
                ));
            }
            for (e, f) in fields.iter().zip(target_fields) {
                check_expression(e.as_ref(), input_schema, f)?;
            }
        }
        Expression::StructPatch(_) | Expression::Opaque(_) | Expression::Unknown(_) => {
            walk_column_refs(expr, input_schema)?;
        }
        _ => {
            let (ty, _) = synthesize(expr, input_schema)?;
            if ty != *want {
                return Err(type_err!(
                    "expression type mismatch for {name:?}: inferred {ty:?}, expected {want:?}",
                    name = expected.name(),
                ));
            }
        }
    }
    Ok(expected.clone())
}

/// Inference variant for `PlanBuilder::project`: each `(name, expr)` becomes a field whose
/// `(data_type, nullable)` come from [`synthesize`]. Returns `(output_schema, pairs)`. Errors on
/// non-inferrable expressions -- route through [`check_projection`] with a declared schema.
pub(crate) fn infer_projection_schema(
    input_schema: &StructType,
    named_exprs: impl IntoIterator<Item = (impl Into<String>, impl Into<ExpressionRef>)>,
) -> SchemaExprResult<(SchemaRef, Vec<(String, ExpressionRef)>)> {
    let pairs: Vec<(String, ExpressionRef)> = named_exprs
        .into_iter()
        .map(|(n, e)| (n.into(), e.into()))
        .collect();
    let fields: Vec<StructField> = pairs
        .iter()
        .map(|(name, expr)| {
            let (ty, nullable) = synthesize(expr.as_ref(), input_schema)?;
            Ok(StructField::new(name.clone(), ty, nullable))
        })
        .collect::<SchemaExprResult<_>>()?;
    Ok((arc_struct_or_invariant(fields)?, pairs))
}

/// Positional pair-up for `PlanBuilder::project_with_schema`: zips `exprs` with `target.fields()`
/// and validates each expression's column references resolve in `input_schema`. The caller's
/// `target` schema is taken as authoritative for the output -- per-expression types are *not*
/// compared against it, which is what lets column-mapping renames and opaque rewrites
/// (`Transform`/`Opaque`/`Unknown`) flow through unchallenged. The runtime engine catches any
/// actual type mismatch.
pub(crate) fn check_projection(
    input_schema: &StructType,
    exprs: impl IntoIterator<Item = impl Into<ExpressionRef>>,
    target: &StructType,
) -> SchemaExprResult<Vec<(String, ExpressionRef)>> {
    let exprs: Vec<ExpressionRef> = exprs.into_iter().map(Into::into).collect();
    let target_fields: Vec<_> = target.fields().collect();
    if exprs.len() != target_fields.len() {
        return Err(type_err!(
            "projection arity mismatch: {got} expressions, {want} target fields",
            got = exprs.len(),
            want = target_fields.len(),
        ));
    }
    for expr in &exprs {
        walk_column_refs(expr.as_ref(), input_schema)?;
    }
    Ok(exprs
        .into_iter()
        .zip(target_fields)
        .map(|(expr, field)| (field.name().clone(), expr))
        .collect())
}

/// Well-formedness check for operator-internal expressions (group-by, sort keys, ...) whose
/// inferred types are discarded. Mirrors `check_projection` semantics: we only verify that every
/// column reference resolves; arbitrary `Binary` / `Transform` / `Opaque` shapes are accepted
/// because their result type does not flow into a target field. Use [`check_expression`] when
/// the inferred type must equal a declared target type, and [`synthesize`] when callers need
/// the result type back.
pub(crate) fn validate_exprs(
    input_schema: &StructType,
    exprs: impl IntoIterator<Item = impl Into<ExpressionRef>>,
) -> SchemaExprResult<()> {
    for expr in exprs {
        walk_column_refs(expr.into().as_ref(), input_schema)?;
    }
    Ok(())
}

/// Identity projection for `PlanBuilder::select`: pairs each target field with `col(name)` and
/// [`check_expression`]-checks each against the input.
pub(crate) fn check_select(
    input_schema: &StructType,
    target: &StructType,
) -> SchemaExprResult<Vec<(String, ExpressionRef)>> {
    target
        .fields()
        .map(|f| {
            let pair = identity_named_expr(f.name().clone());
            check_expression(pair.1.as_ref(), input_schema, f)?;
            Ok(pair)
        })
        .collect()
}

/// Verify every column reference in `expr` resolves in `schema`. Module-scoped because
/// [`ExpressionTransform`] requires a named type.
fn walk_column_refs(expr: &Expression, schema: &StructType) -> SchemaExprResult<()> {
    struct Validator<'a> {
        schema: &'a StructType,
        err: Option<SchemaExprError>,
    }
    impl<'a> ExpressionTransform<'a> for Validator<'a> {
        fn transform_expr_column(&mut self, name: &'a ColumnName) -> Option<Cow<'a, ColumnName>> {
            if self.err.is_none() {
                if let Err(e) = self.schema.walk_column_fields(name) {
                    self.err = Some(type_err!("unresolved column reference {name}: {e}"));
                }
            }
            Some(Cow::Borrowed(name))
        }
    }
    let mut v = Validator { schema, err: None };
    let _ = v.transform_expr(expr);
    v.err.map_or(Ok(()), Err)
}

/// Strict-equality unification of N inferred types. Empty input is rejected (operators always
/// build with >=1 arm).
fn unify_types(mut types: impl Iterator<Item = DataType>, op: &str) -> SchemaExprResult<DataType> {
    types.all_equal_value().map_err(|d| match d {
        None => type_err!("{op}: cannot infer type with zero arms"),
        Some((a, b)) => type_err!("{op}: arm types disagree -- got {a:?} and {b:?}"),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;

    use super::*;
    use crate::expressions::{col, lit, Expression, Predicate};
    use crate::schema::{MapType, StructField, StructType};

    fn input_schema() -> StructType {
        let map = DataType::Map(Box::new(MapType::new(
            DataType::STRING,
            DataType::STRING,
            true,
        )));
        StructType::try_new(vec![
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("ts", DataType::LONG),
            StructField::nullable("flag", DataType::BOOLEAN),
            StructField::nullable("tags", map),
        ])
        .unwrap()
    }

    /// Build a Struct `DataType` from `(name, type)` pairs.
    fn make_struct<const N: usize>(fields: [(&str, DataType); N]) -> DataType {
        let fields: Vec<_> = fields
            .into_iter()
            .map(|(n, t)| StructField::nullable(n, t))
            .collect();
        DataType::Struct(Box::new(StructType::try_new(fields).unwrap()))
    }

    fn long_long_struct() -> DataType {
        make_struct([("a", DataType::LONG), ("b", DataType::LONG)])
    }

    fn long_string_struct(a: &str, b: &str) -> DataType {
        make_struct([(a, DataType::LONG), (b, DataType::STRING)])
    }

    /// Build a nullable target field named `"out"` of the given type.
    fn target(ty: DataType) -> StructField {
        StructField::nullable("out", ty)
    }

    // === synthesize: happy paths ===
    #[rstest]
    #[case::literal_long(lit(42i64), DataType::LONG, false)]
    #[case::literal_null(Expression::null_literal(DataType::STRING), DataType::STRING, true)]
    #[case::column_string(col("id"), DataType::STRING, true)]
    #[case::column_long(col("ts"), DataType::LONG, true)]
    #[case::predicate_boolean(Expression::from_pred(Predicate::column(["flag"])), DataType::BOOLEAN, true)]
    #[case::to_json_propagates_nullable(col("id").to_json(), DataType::STRING, true)]
    #[case::coalesce_all_nullable(Expression::coalesce([col("id"), col("id")]), DataType::STRING, true)]
    #[case::coalesce_with_literal_non_null(Expression::coalesce([col("id"), lit("default")]), DataType::STRING, false)]
    fn synthesize_happy(#[case] expr: Expression, #[case] ty: DataType, #[case] nullable: bool) {
        assert_eq!(synthesize(&expr, &input_schema()).unwrap(), (ty, nullable));
    }

    // === synthesize: errors ===
    #[rstest]
    #[case::unknown_column(col("missing"), "missing")]
    #[case::coalesce_disagree(Expression::coalesce([col("id"), col("ts")]), "disagree")]
    #[case::bidirectional_only(Expression::unknown("opaque"), "target field")]
    fn synthesize_errors(#[case] expr: Expression, #[case] needle: &str) {
        let err = synthesize(&expr, &input_schema()).unwrap_err().to_string();
        assert!(err.contains(needle), "expected {needle:?}, got: {err}");
    }

    #[test]
    fn array_outer_non_null_inner_contains_null_conservative() {
        let (ty, nullable) =
            synthesize(&Expression::array([col("id"), lit("x")]), &input_schema()).unwrap();
        assert!(!nullable);
        let DataType::Array(arr) = ty else {
            panic!("expected Array")
        };
        assert_eq!(arr.element_type, DataType::STRING);
        assert!(arr.contains_null);
    }

    // === check_expression: bidirectional rules ===
    //
    // Each case runs `check_expression(expr, input_schema, target(ty))` and either expects
    // success (`None`) or an error containing the given substring.
    #[rstest]
    #[case::col_match_ok(col("ts"), DataType::LONG, None)]
    #[case::col_type_mismatch(col("ts"), DataType::STRING, Some("expression type mismatch"))]
    #[case::map_to_struct_ok(Expression::map_to_struct(col("tags")), long_long_struct(), None)]
    #[case::map_to_struct_bad_input(
        Expression::map_to_struct(col("id")),
        long_long_struct(),
        Some("Map<String, String>")
    )]
    #[case::map_to_struct_bad_target(
        Expression::map_to_struct(col("tags")),
        DataType::LONG,
        Some("expected Struct")
    )]
    #[case::struct_arity(Expression::struct_from([Arc::new(col("ts"))]), long_long_struct(), Some("arity mismatch"))]
    #[case::struct_ok(Expression::struct_from([Arc::new(col("ts")), Arc::new(col("ts"))]), long_long_struct(), None)]
    #[case::struct_field_mismatch(Expression::struct_from([Arc::new(col("ts")), Arc::new(col("id"))]), long_long_struct(), Some("expression type mismatch"))]
    #[case::unknown_accepts_any(Expression::unknown("opaque"), DataType::LONG, None)]
    fn check_expression_cases(
        #[case] expr: Expression,
        #[case] ty: DataType,
        #[case] err: Option<&str>,
    ) {
        let s = input_schema();
        let t = target(ty);
        let result = check_expression(&expr, &s, &t);
        match err {
            None => assert_eq!(result.unwrap().name(), "out"),
            Some(needle) => {
                let e = result.unwrap_err().to_string();
                assert!(e.contains(needle), "expected {needle:?}, got: {e}");
            }
        }
    }

    /// The caller's `nullable` is authoritative -- the returned field mirrors `expected.nullable`
    /// even when the expression itself synthesizes as nullable.
    #[test]
    fn check_expression_returns_caller_nullable() {
        let got = check_expression(
            &col("ts"),
            &input_schema(),
            &StructField::not_null("out", DataType::LONG),
        )
        .unwrap();
        assert!(!got.nullable);
    }

    // === infer_projection_schema ===
    #[test]
    fn infer_projection_schema_builds_inferred_fields() {
        let pairs_in = [("out_id", col("id")), ("out_ts", col("ts"))];
        let (out, pairs) = infer_projection_schema(&input_schema(), pairs_in).unwrap();
        assert_eq!(out.field("out_id").unwrap().data_type(), &DataType::STRING);
        assert!(out.field("out_id").unwrap().nullable);
        assert_eq!(out.field("out_ts").unwrap().data_type(), &DataType::LONG);
        assert!(out.field("out_ts").unwrap().nullable);
        let names: Vec<&str> = pairs.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["out_id", "out_ts"]);
    }

    /// Non-null literals + `coalesce(nullable, non_null)` both produce non-null projected fields.
    #[test]
    fn infer_projection_schema_propagates_nullability() {
        let pairs = [
            ("from_literal", lit(42i64)),
            (
                "from_coalesce",
                Expression::coalesce([col("id"), lit("default")]),
            ),
        ];
        let (out, _) = infer_projection_schema(&input_schema(), pairs).unwrap();
        assert!(!out.field("from_literal").unwrap().nullable);
        assert!(!out.field("from_coalesce").unwrap().nullable);
    }

    // === check_projection ===
    #[test]
    fn check_projection_arity_mismatch_is_caught() {
        let t = StructType::try_new(vec![StructField::nullable("only", DataType::STRING)]).unwrap();
        let exprs: Vec<ExpressionRef> = vec![Arc::new(col("id")), Arc::new(col("ts"))];
        let err = check_projection(&input_schema(), exprs, &t)
            .unwrap_err()
            .to_string();
        assert!(err.contains("projection arity mismatch"));
    }

    #[test]
    fn check_projection_returns_target_named_pairs() {
        let s = StructType::try_new(vec![StructField::nullable(
            "src",
            long_string_struct("phys_a", "phys_b"),
        )])
        .unwrap();
        let t = StructType::try_new(vec![StructField::nullable(
            "renamed",
            long_string_struct("log_a", "log_b"),
        )])
        .unwrap();
        let pairs = check_projection(&s, [Arc::new(col("src")) as ExpressionRef], &t).unwrap();
        assert_eq!(
            pairs.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
            ["renamed"]
        );
    }
}
