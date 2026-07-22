//! Conversion from a kernel [`Expression`] to a DataFusion [`Expr`].

use datafusion::common::Column;
use datafusion::functions::core::expr_fn::{coalesce, get_field_path};
use datafusion::functions_nested::expr_fn::make_array;
use datafusion::logical_expr::{binary_expr, lit, Expr, Operator};
use delta_kernel::expressions::{
    BinaryExpression, BinaryExpressionOp, ColumnName, Expression, UnaryExpressionOp,
    VariadicExpression, VariadicExpressionOp,
};
use delta_kernel::schema::StructType;
use delta_kernel::{DeltaResult, Error};

use crate::scalar::kernel_to_df_scalar;

/// Converts a kernel [`Expression`] into the equivalent DataFusion [`Expr`].
///
/// # Errors
/// Returns an error for a column that does not resolve against `input_schema`, and
/// [`Error::unsupported`] for arms with no untyped DataFusion equivalent (see the `TODO`s below).
pub fn kernel_to_datafusion_expr(expr: &Expression, input_schema: &StructType) -> DeltaResult<Expr> {
    match expr {
        Expression::Literal(scalar) => Ok(lit(kernel_to_df_scalar(scalar)?)),
        Expression::Column(name) => kernel_column_to_df_expr(name, input_schema),
        Expression::Binary(binary) => kernel_binary_expr_to_df_expr(binary, input_schema),
        Expression::Variadic(variadic) => kernel_variadic_to_df_expr(variadic, input_schema),

        // TODO: wire up in the predicate-conversion PR (needs the `Predicate -> Expr` converter).
        Expression::Predicate(_) => Err(Error::unsupported(
            "converting an embedded Predicate expression is not yet supported",
        )),

        // TODO: wire up once this function takes an output schema (`Struct` needs it for field
        // names; `MapToStruct`/`StructPatch` for field types). Each arm's lowering follows later.
        Expression::Struct(_, _)
        | Expression::MapToStruct(_)
        | Expression::StructPatch(_) => Err(Error::unsupported(
            "converting schema-dependent expressions (Struct, MapToStruct, StructPatch) requires \
             a typed projection context",
        )),

        // TODO: wire up via a custom JSON-parsing UDF (DataFusion core has no stock JSON parser).
        Expression::ParseJson(_) => Err(Error::unsupported(
            "converting a ParseJson expression requires a custom JSON-parsing UDF",
        )),

        Expression::Unary(u) => match u.op {
            UnaryExpressionOp::ToJson => Err(Error::unsupported(
                "converting the ToJson expression is not yet supported",
            )),
        },

        Expression::Opaque(_) => Err(Error::unsupported(
            "cannot convert an engine-defined Opaque expression",
        )),
        Expression::Unknown(name) => Err(Error::unsupported(format!(
            "cannot convert Unknown expression {name:?}"
        ))),
    }
}

/// Lowers a column reference to a nested field access, e.g. `a.b.c` becomes a single
/// `get_field(col("a"), "b", "c")` call. The path is resolved against `input_schema` (via
/// [`StructType::field_at`]) to fail fast, but the resolved field is otherwise unused.
fn kernel_column_to_df_expr(name: &ColumnName, input_schema: &StructType) -> DeltaResult<Expr> {
    input_schema.field_at(name)?;
    let mut path = name.iter();
    let root = path
        .next()
        .ok_or_else(|| Error::generic("cannot convert an empty column reference"))?;
    let root = Expr::Column(Column::new_unqualified(root));
    let field_names = path.map(lit).collect::<Vec<_>>();
    // A bare column stays a bare column; only nested access wraps it in a `get_field` call.
    Ok(if field_names.is_empty() {
        root
    } else {
        get_field_path(root, field_names)
    })
}

/// Lowers an arithmetic binary expression (`Plus`/`Minus`/`Multiply`/`Divide`) to an
/// `Expr::BinaryExpr`. Comparison and `IN` operators are modeled as predicates, not expressions,
/// so they never reach this arm.
fn kernel_binary_expr_to_df_expr(binary: &BinaryExpression, input_schema: &StructType) -> DeltaResult<Expr> {
    let op = match binary.op {
        BinaryExpressionOp::Plus => Operator::Plus,
        BinaryExpressionOp::Minus => Operator::Minus,
        BinaryExpressionOp::Multiply => Operator::Multiply,
        BinaryExpressionOp::Divide => Operator::Divide,
    };
    let left = kernel_to_datafusion_expr(&binary.left, input_schema)?;
    let right = kernel_to_datafusion_expr(&binary.right, input_schema)?;
    Ok(binary_expr(left, op, right))
}

/// Lowers a variadic expression: `Coalesce` to `coalesce(..)` and `Array` to `make_array(..)`,
/// each over the converted arguments.
fn kernel_variadic_to_df_expr(variadic: &VariadicExpression, input_schema: &StructType) -> DeltaResult<Expr> {
    let args = variadic
        .exprs
        .iter()
        .map(|e| kernel_to_datafusion_expr(e, input_schema))
        .collect::<DeltaResult<Vec<_>>>()?;
    Ok(match variadic.op {
        VariadicExpressionOp::Coalesce => coalesce(args),
        VariadicExpressionOp::Array => make_array(args),
    })
}

#[cfg(test)]
mod tests {
    use delta_kernel::expressions::{column_expr, Expression as Expr_};
    use delta_kernel::schema::{DataType, StructField, StructType};
    use rstest::rstest;

    use super::*;

    /// Name-resolution scope for these tests: `a: { b: { c: long } }`, plus top-level `b` and `x`.
    fn test_schema() -> StructType {
        StructType::try_new([
            StructField::nullable(
                "a",
                StructType::try_new([StructField::nullable(
                    "b",
                    StructType::try_new([StructField::nullable("c", DataType::LONG)]).unwrap(),
                )])
                .unwrap(),
            ),
            StructField::nullable("b", DataType::LONG),
            StructField::nullable("x", DataType::LONG),
        ])
        .unwrap()
    }

    /// Lowers an expression against [`test_schema`] and renders it as a DataFusion `Display`
    /// string.
    fn lower(expr: Expr_) -> String {
        kernel_to_datafusion_expr(&expr, &test_schema())
            .unwrap()
            .to_string()
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
        "get_field(a, Utf8(\"b\"), Utf8(\"c\"))"
    )]
    fn column_lowers_to_nested_field_access(#[case] kernel: Expr_, #[case] expected: &str) {
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

    /// A column reference that does not resolve against the input schema fails at conversion time,
    /// not later during DataFusion analysis. Covers each `field_at` failure mode.
    #[rstest]
    #[case::empty(Expr_::Column(ColumnName::new(Vec::<String>::new())))]
    #[case::unknown_root(Expr_::column(["nope"]))]
    #[case::unknown_nested(Expr_::column(["a", "b", "missing"]))]
    #[case::descend_into_non_struct(Expr_::column(["x", "y"]))]
    fn unresolved_column_is_an_error(#[case] kernel: Expr_) {
        kernel_to_datafusion_expr(&kernel, &test_schema()).unwrap_err();
    }
}
