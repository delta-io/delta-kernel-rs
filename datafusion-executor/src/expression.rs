//! Conversion from a kernel [`Expression`](KernelExpression) to a DataFusion [`Expr`](DFExpr).

use datafusion::common::Column as DFColumn;
use datafusion::functions::core::expr_fn::{coalesce, get_field_path};
use datafusion::functions_nested::expr_fn::make_array;
use datafusion::logical_expr::{binary_expr, lit, Expr as DFExpr, Operator};
use delta_kernel::expressions::{
    BinaryExpression, BinaryExpressionOp, ColumnName as KernelColumnName,
    Expression as KernelExpression, UnaryExpressionOp, VariadicExpression, VariadicExpressionOp,
};
use delta_kernel::schema::StructType;
use delta_kernel::{DeltaResult, Error};

use crate::scalar::to_df_scalar;

/// Converts a kernel [`Expression`](KernelExpression) into the equivalent DataFusion
/// [`Expr`](DFExpr).
///
/// # Errors
/// Returns an error for a column that does not resolve against `input_schema`, and
/// [`Error::unsupported`] for arms with no untyped DataFusion equivalent (see the `TODO`s below).
pub fn to_df_expr(expr: &KernelExpression, input_schema: &StructType) -> DeltaResult<DFExpr> {
    match expr {
        KernelExpression::Literal(scalar) => Ok(lit(to_df_scalar(scalar)?)),
        KernelExpression::Column(name) => column_to_df_expr(name, input_schema),
        KernelExpression::Binary(binary) => binary_expr_to_df_expr(binary, input_schema),
        KernelExpression::Variadic(variadic) => variadic_to_df_expr(variadic, input_schema),

        // TODO: wire up in the predicate-conversion PR (needs the `Predicate -> Expr` converter).
        KernelExpression::Predicate(_) => Err(Error::unsupported(
            "converting an embedded Predicate expression is not yet supported",
        )),

        // TODO: wire up once this function takes an output schema (`Struct` needs it for field
        // names; `MapToStruct`/`StructPatch` for field types). Each arm's lowering follows later.
        KernelExpression::Struct(_, _)
        | KernelExpression::MapToStruct(_)
        | KernelExpression::StructPatch(_) => Err(Error::unsupported(
            "converting schema-dependent expressions (Struct, MapToStruct, StructPatch) \
                 requires a typed projection context",
        )),

        // TODO: wire up via a custom JSON-parsing UDF (DataFusion core has no stock JSON parser).
        KernelExpression::ParseJson(_) => Err(Error::unsupported(
            "converting a ParseJson expression requires a custom JSON-parsing UDF",
        )),

        KernelExpression::Unary(u) => match u.op {
            UnaryExpressionOp::ToJson => Err(Error::unsupported(
                "converting the ToJson expression is not yet supported",
            )),
        },

        // TODO(#3007): implement once kernel's Cast semantics are clarified.
        KernelExpression::Cast(_) => Err(Error::unsupported(
            "converting a Cast expression is not yet supported",
        )),

        KernelExpression::Opaque(_) => Err(Error::unsupported(
            "cannot convert an engine-defined Opaque expression",
        )),
        KernelExpression::Unknown(name) => Err(Error::unsupported(format!(
            "cannot convert Unknown expression {name:?}"
        ))),
    }
}

/// Lowers a column reference to a nested field access, e.g. `a.b.c` becomes a single
/// `get_field(col("a"), "b", "c")` call. The path is resolved against `input_schema` (via
/// [`StructType::field_at`]) to fail fast, but the resolved field is otherwise unused.
fn column_to_df_expr(name: &KernelColumnName, input_schema: &StructType) -> DeltaResult<DFExpr> {
    let _ = input_schema.field_at(name)?;
    let mut path = name.iter();
    let Some(root) = path.next() else {
        return Err(Error::generic("cannot convert an empty column reference"));
    };
    let root = DFExpr::Column(DFColumn::new_unqualified(root));
    let field_names = Vec::from_iter(path.map(lit));
    // A bare column stays a bare column; only nested access wraps it in a `get_field` call.
    if field_names.is_empty() {
        Ok(root)
    } else {
        Ok(get_field_path(root, field_names))
    }
}

/// Lowers an arithmetic binary expression (`Plus`/`Minus`/`Multiply`/`Divide`) to an
/// `Expr::BinaryExpr`. Comparison and `IN` operators are modeled as predicates, not expressions,
/// so they never reach this arm.
fn binary_expr_to_df_expr(
    binary: &BinaryExpression,
    input_schema: &StructType,
) -> DeltaResult<DFExpr> {
    let op = match binary.op {
        BinaryExpressionOp::Plus => Operator::Plus,
        BinaryExpressionOp::Minus => Operator::Minus,
        BinaryExpressionOp::Multiply => Operator::Multiply,
        BinaryExpressionOp::Divide => Operator::Divide,
    };
    let left = to_df_expr(&binary.left, input_schema)?;
    let right = to_df_expr(&binary.right, input_schema)?;
    Ok(binary_expr(left, op, right))
}

/// Lowers a variadic expression: `Coalesce` to `coalesce(..)` and `Array` to `make_array(..)`,
/// each over the converted arguments.
fn variadic_to_df_expr(
    variadic: &VariadicExpression,
    input_schema: &StructType,
) -> DeltaResult<DFExpr> {
    let args: DeltaResult<Vec<DFExpr>> = variadic
        .exprs
        .iter()
        .map(|e| to_df_expr(e, input_schema))
        .collect();
    Ok(match variadic.op {
        VariadicExpressionOp::Coalesce => coalesce(args?),
        VariadicExpressionOp::Array => make_array(args?),
    })
}

#[cfg(test)]
mod tests {
    use delta_kernel::expressions::{column_expr, Expression as KernelExpr};
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
    fn lower(expr: KernelExpr) -> String {
        to_df_expr(&expr, &test_schema()).unwrap().to_string()
    }

    #[rstest]
    #[case::i32(KernelExpr::literal(7i32), "Int32(7)")]
    #[case::i64(KernelExpr::literal(42i64), "Int64(42)")]
    #[case::string(KernelExpr::literal("abc"), "Utf8(\"abc\")")]
    #[case::boolean(KernelExpr::literal(true), "Boolean(true)")]
    #[case::null(KernelExpr::null_literal(DataType::LONG), "Int64(NULL)")]
    fn literal_lowers_to_scalar(#[case] kernel: KernelExpr, #[case] expected: &str) {
        assert_eq!(lower(kernel), expected);
    }

    #[rstest]
    #[case::single(KernelExpr::column(["a"]), "a")]
    #[case::depth_2(KernelExpr::column(["a", "b"]), "get_field(a, Utf8(\"b\"))")]
    #[case::depth_3(
        KernelExpr::column(["a", "b", "c"]),
        "get_field(a, Utf8(\"b\"), Utf8(\"c\"))"
    )]
    fn column_lowers_to_nested_field_access(#[case] kernel: KernelExpr, #[case] expected: &str) {
        assert_eq!(lower(kernel), expected);
    }

    #[rstest]
    #[case::plus(column_expr!("a") + KernelExpr::literal(1i64), "a + Int64(1)")]
    #[case::minus(column_expr!("a") - KernelExpr::literal(1i64), "a - Int64(1)")]
    #[case::multiply(column_expr!("a") * KernelExpr::literal(2i64), "a * Int64(2)")]
    #[case::divide(column_expr!("a") / KernelExpr::literal(2i64), "a / Int64(2)")]
    fn arithmetic_binary_lowers_to_binary_expr(#[case] kernel: KernelExpr, #[case] expected: &str) {
        assert_eq!(lower(kernel), expected);
    }

    /// Nested arithmetic lowers to the matching operator tree.
    #[rstest]
    #[case::precedence_pins_grouping(
        (column_expr!("x") + KernelExpr::literal(1i64)) * (column_expr!("b") - KernelExpr::literal(2i64)),
        "(x + Int64(1)) * (b - Int64(2))"
    )]
    #[case::nested_field_and_all_ops(
        (KernelExpr::column(["a", "b", "c"]) * KernelExpr::literal(5i64)
            - (column_expr!("b") + column_expr!("x")))
            / KernelExpr::literal(20i64),
        "(get_field(a, Utf8(\"b\"), Utf8(\"c\")) * Int64(5) - b + x) / Int64(20)"
    )]
    fn nested_arithmetic_lowers_to_operator_tree(#[case] kernel: KernelExpr, #[case] expected: &str) {
        assert_eq!(lower(kernel), expected);
    }

    #[rstest]
    #[case::coalesce(
        KernelExpr::coalesce([column_expr!("a"), column_expr!("b"), KernelExpr::literal(0i64)]),
        "coalesce(a, b, Int64(0))"
    )]
    #[case::array(
        KernelExpr::array([KernelExpr::literal(1i64), KernelExpr::literal(2i64)]),
        "make_array(Int64(1), Int64(2))"
    )]
    #[case::nested_coalesce(
        KernelExpr::coalesce([KernelExpr::coalesce([column_expr!("a"), column_expr!("b")]), column_expr!("x")]),
        "coalesce(coalesce(a, b), x)"
    )]
    #[case::nested_array(
        KernelExpr::array([
            KernelExpr::array([KernelExpr::literal(1i64), KernelExpr::literal(2i64)]),
            KernelExpr::array([KernelExpr::literal(3i64), KernelExpr::literal(4i64)]),
        ]),
        "make_array(make_array(Int64(1), Int64(2)), make_array(Int64(3), Int64(4)))"
    )]
    fn variadic_lowers_to_call(#[case] kernel: KernelExpr, #[case] expected: &str) {
        assert_eq!(lower(kernel), expected);
    }

    /// A column reference that does not resolve against the input schema fails at conversion time,
    /// not later during DataFusion analysis. Covers each `field_at` failure mode.
    #[rstest]
    #[case::empty(KernelExpr::Column(KernelColumnName::default()))]
    #[case::unknown_root(KernelExpr::column(["nope"]))]
    #[case::unknown_nested(KernelExpr::column(["a", "b", "missing"]))]
    #[case::descend_into_non_struct(KernelExpr::column(["x", "y"]))]
    fn unresolved_column_is_an_error(#[case] kernel: KernelExpr) {
        to_df_expr(&kernel, &test_schema()).unwrap_err();
    }
}
