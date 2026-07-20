//! Conversion from a kernel [`Expression`] to a DataFusion [`Expr`].
//!
//! One match arm per [`Expression`] variant, each recursing on its children. Leaf arms
//! (`Literal`, `Column`) bottom out directly; compound arms (`Binary`, `Variadic`, `Struct`)
//! rebuild the equivalent DataFusion node from converted children.
//!
//! This conversion is **untyped**: it maps an expression to its natural DataFusion shape with no
//! target output field. Arms that can only be lowered against a target schema -- casts, JSON
//! parsing, map-to-struct reshaping, sparse struct patches -- return [`Error::unsupported`] until
//! the typed `Project`-node compiler that supplies that context is wired up.
//!
//! An `impl TryFrom<&Expression> for Expr` is impossible here: both types are foreign to this
//! crate, so the orphan rule forbids it. Hence a free function.

use datafusion::common::Column;
use datafusion::functions::core::expr_fn::{coalesce, get_field};
use datafusion::functions_nested::expr_fn::make_array;
use datafusion::logical_expr::{binary_expr, lit, Expr, Operator};
use delta_kernel::expressions::{
    BinaryExpression, BinaryExpressionOp, ColumnName, Expression, VariadicExpression,
    VariadicExpressionOp,
};
use delta_kernel::{DeltaResult, Error};

use crate::scalar::kernel_to_df_scalar;

/// Converts a kernel [`Expression`] into the equivalent DataFusion [`Expr`].
///
/// # Errors
///
/// Returns [`Error::unsupported`] for expressions that have no untyped DataFusion equivalent:
/// engine-defined (`Opaque`) or opaque-to-both (`Unknown`) expressions, the `ToJson` unary op,
/// the embedded-predicate arm (until the predicate converter lands), and the schema-dependent
/// arms (`Struct`, `ParseJson`, `MapToStruct`, `StructPatch`). Also propagates any error from
/// converting a child scalar (e.g. an interval literal, which has no Arrow representation) or an
/// empty column reference.
pub fn to_datafusion_expr(expr: &Expression) -> DeltaResult<Expr> {
    match expr {
        Expression::Literal(scalar) => Ok(lit(kernel_to_df_scalar(scalar)?)),
        Expression::Column(name) => column_to_expr(name),
        Expression::Binary(binary) => binary_to_expr(binary),
        Expression::Variadic(variadic) => variadic_to_expr(variadic),
        // A boolean-valued predicate used where a value is expected. Wired up by the
        // predicate-conversion branch, which supplies the `Predicate -> Expr` converter.
        Expression::Predicate(_) => Err(Error::unsupported(
            "converting an embedded Predicate expression is not yet supported",
        )),
        Expression::Unary(_) => Err(Error::unsupported(
            "converting the ToJson expression is not yet supported",
        )),
        // Engine-defined and opaque-to-both expressions cannot round-trip through a DataFusion
        // logical plan: kernel only understands them through their trait methods.
        Expression::Opaque(_) => Err(Error::unsupported(
            "cannot convert an engine-defined Opaque expression",
        )),
        Expression::Unknown(name) => Err(Error::unsupported(format!(
            "cannot convert Unknown expression {name:?}"
        ))),
        // These lower correctly only against a target output schema, which the untyped conversion
        // does not carry, so they are deferred to the typed `Project`-node compiler. `Struct` is
        // deferred too: its field values are self-contained, but its field *names* live in the
        // target schema, and a struct built here would only ever be re-labelled by that compiler
        // -- so lowering it in isolation produces a value nothing executes as-is.
        Expression::Struct(_, _)
        | Expression::ParseJson(_)
        | Expression::MapToStruct(_)
        | Expression::StructPatch(_) => Err(Error::unsupported(
            "converting schema-dependent expressions (Struct, ParseJson, MapToStruct, \
             StructPatch) requires a typed projection context",
        )),
    }
}

/// Lowers a column reference to a `get_field` chain rooted at the first path segment, e.g. `a.b.c`
/// becomes `get_field(get_field(col("a"), "b"), "c")`. A single-segment path is just the root
/// column.
///
/// # Errors
///
/// Returns [`Error::unsupported`] for an empty column path, which has no column to root on.
fn column_to_expr(name: &ColumnName) -> DeltaResult<Expr> {
    let mut path = name.iter();
    let root = path
        .next()
        .ok_or_else(|| Error::unsupported("cannot convert an empty column reference"))?;
    let root = Expr::Column(Column::new_unqualified(root));
    Ok(path.fold(root, get_field))
}

/// Lowers an arithmetic binary expression (`Plus`/`Minus`/`Multiply`/`Divide`) to an
/// `Expr::BinaryExpr`. Comparison and `IN` operators are modeled as predicates, not expressions,
/// so they never reach this arm.
fn binary_to_expr(binary: &BinaryExpression) -> DeltaResult<Expr> {
    let op = match binary.op {
        BinaryExpressionOp::Plus => Operator::Plus,
        BinaryExpressionOp::Minus => Operator::Minus,
        BinaryExpressionOp::Multiply => Operator::Multiply,
        BinaryExpressionOp::Divide => Operator::Divide,
    };
    let left = to_datafusion_expr(&binary.left)?;
    let right = to_datafusion_expr(&binary.right)?;
    Ok(binary_expr(left, op, right))
}

/// Lowers a variadic expression: `Coalesce` to `coalesce(..)` and `Array` to `make_array(..)`,
/// each over the converted arguments.
fn variadic_to_expr(variadic: &VariadicExpression) -> DeltaResult<Expr> {
    let args = variadic
        .exprs
        .iter()
        .map(to_datafusion_expr)
        .collect::<DeltaResult<Vec<_>>>()?;
    Ok(match variadic.op {
        VariadicExpressionOp::Coalesce => coalesce(args),
        VariadicExpressionOp::Array => make_array(args),
    })
}

#[cfg(test)]
mod tests {
    use delta_kernel::expressions::{column_expr, Expression as Expr_};
    use delta_kernel::schema::DataType;
    use rstest::rstest;

    use super::*;

    /// Lowers an expression and renders it as a DataFusion `Display` string for comparison.
    fn lower(expr: Expr_) -> String {
        to_datafusion_expr(&expr).unwrap().to_string()
    }

    #[rstest]
    #[case::i32(Expr_::literal(7i32), "Int32(7)")]
    #[case::i64(Expr_::literal(42i64), "Int64(42)")]
    #[case::string(Expr_::literal("abc"), "Utf8(\"abc\")")]
    #[case::boolean(Expr_::literal(true), "Boolean(true)")]
    #[case::null(Expr_::null_literal(DataType::LONG), "Int64(NULL)")]
    fn literal_lowers_to_scalar(#[case] kernel: Expr_, #[case] expected: &str) {
        assert_eq!(lower(kernel), expected);
    }

    #[rstest]
    #[case::single(Expr_::column(["a"]), "a")]
    #[case::depth_2(Expr_::column(["a", "b"]), "get_field(a, Utf8(\"b\"))")]
    #[case::depth_3(
        Expr_::column(["a", "b", "c"]),
        "get_field(get_field(a, Utf8(\"b\")), Utf8(\"c\"))"
    )]
    fn column_lowers_to_get_field_chain(#[case] kernel: Expr_, #[case] expected: &str) {
        assert_eq!(lower(kernel), expected);
    }

    #[rstest]
    #[case::plus(column_expr!("a") + Expr_::literal(1i64), "a + Int64(1)")]
    #[case::minus(column_expr!("a") - Expr_::literal(1i64), "a - Int64(1)")]
    #[case::multiply(column_expr!("a") * Expr_::literal(2i64), "a * Int64(2)")]
    #[case::divide(column_expr!("a") / Expr_::literal(2i64), "a / Int64(2)")]
    fn arithmetic_binary_lowers_to_binary_expr(#[case] kernel: Expr_, #[case] expected: &str) {
        assert_eq!(lower(kernel), expected);
    }

    #[test]
    fn nested_arithmetic_preserves_grouping() {
        let kernel = (column_expr!("x") + Expr_::literal(4i64)) * Expr_::literal(10i64);
        assert_eq!(lower(kernel), "(x + Int64(4)) * Int64(10)");
    }

    #[test]
    fn coalesce_lowers_to_coalesce_call() {
        let kernel = Expr_::coalesce([column_expr!("a"), column_expr!("b"), Expr_::literal(0i64)]);
        assert_eq!(lower(kernel), "coalesce(a, b, Int64(0))");
    }

    #[test]
    fn array_lowers_to_make_array_call() {
        let kernel = Expr_::array([Expr_::literal(1i64), Expr_::literal(2i64)]);
        assert_eq!(lower(kernel), "make_array(Int64(1), Int64(2))");
    }

    #[test]
    fn struct_is_deferred_to_typed_projection() {
        // A struct's field values are convertible, but its field names come from the target
        // schema this untyped conversion does not carry, so `Struct` is deferred to the typed
        // `Project` compiler rather than lowered to an anonymously-named `struct(..)`.
        let kernel = Expr_::struct_from([column_expr!("a"), Expr_::literal(1i64)]);
        to_datafusion_expr(&kernel).unwrap_err();
    }

    #[test]
    fn embedded_predicate_is_unsupported() {
        let kernel = Expr_::Predicate(Box::new(column_expr!("a").is_null()));
        to_datafusion_expr(&kernel).unwrap_err();
    }

    #[test]
    fn unknown_expression_is_unsupported() {
        to_datafusion_expr(&Expr_::Unknown("mystery".into())).unwrap_err();
    }
}
