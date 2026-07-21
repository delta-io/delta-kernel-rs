//! Conversion from a kernel [`Expression`] to a DataFusion [`Expr`].
//!
//! One match arm per [`Expression`] variant, each recursing on its children. Leaf arms
//! (`Literal`, `Column`) bottom out directly; compound arms (`Binary`, `Variadic`) rebuild the
//! equivalent DataFusion node from converted children.
//!
//! This conversion is **untyped**: it maps an expression to its natural DataFusion shape with no
//! target output field. It still takes the input schema, but only to fail fast: a column reference
//! is validated against it (the path must resolve through nested structs) and then lowered to a
//! `col(..)`/`get_field(..)` chain. DataFusion would resolve that chain against the upstream schema
//! during plan analysis regardless; validating here turns a dangling reference into a clear error
//! at conversion time instead of a late, less-legible analysis failure.
//!
//! The variants group by why they are or are not lowerable in this untyped conversion:
//!
//! 1. **Natural type** (`Literal`, `Column`, `Binary`, `Variadic`): self-describing; lowered here.
//! 2. **Predicate** (`Predicate`): a boolean-valued predicate used as a value. Deferred to the
//!    predicate-conversion branch, which supplies the `Predicate -> Expr` converter.
//! 3. **Typed** (`Struct`, `MapToStruct`, `StructPatch`): lowerable only against a target output
//!    schema -- for field *names* (`Struct`) and/or field *types* (`MapToStruct`, `StructPatch`) --
//!    which this untyped conversion does not carry. Deferred to the typed `Project`-node compiler.
//! 4. **ParseJson**: carries its own `output_schema`, so it is not blocked on a target schema. It
//!    is unsupported because DataFusion core has no stock JSON-string -> struct parser; lowering it
//!    needs a custom UDF that mirrors kernel's `parse_json` decoder, which is not yet wired up.
//! 5. **Terminal** (`Unary(ToJson)`, `Opaque`, `Unknown`): no faithful untyped lowering. `ToJson`
//!    needs a UDF kernel has not wired; `Opaque` is engine-defined and understood only through its
//!    trait methods; `Unknown` has no semantics to lower (kernel forbids interpreting it).
//!
//! An `impl TryFrom<&Expression> for Expr` is impossible here: both types are foreign to this
//! crate, so the orphan rule forbids it. Hence a free function.

use datafusion::common::Column;
use datafusion::functions::core::expr_fn::{coalesce, get_field};
use datafusion::functions_nested::expr_fn::make_array;
use datafusion::logical_expr::{binary_expr, lit, Expr, Operator};
use delta_kernel::expressions::{
    BinaryExpression, BinaryExpressionOp, ColumnName, Expression, UnaryExpressionOp,
    VariadicExpression, VariadicExpressionOp,
};
use delta_kernel::schema::StructType;
use delta_kernel::{DeltaResult, Error};

use crate::scalar::kernel_to_df_scalar;

/// Converts a kernel [`Expression`] into the equivalent DataFusion [`Expr`], validating column
/// references against `input_schema` (the name-resolution scope: a column path must resolve
/// through the nested structs of this schema).
///
/// # Errors
///
/// Returns an error for a column reference that does not resolve against `input_schema`, and
/// [`Error::unsupported`] for expressions that have no untyped DataFusion equivalent:
/// engine-defined (`Opaque`) or opaque-to-both (`Unknown`) expressions, the `ToJson` unary op,
/// the embedded-predicate arm (until the predicate converter lands), the schema-dependent arms
/// (`Struct`, `MapToStruct`, `StructPatch`), and `ParseJson` (which lacks a stock DataFusion
/// lowering, not a schema). Also propagates any error from converting a child scalar (e.g. an
/// interval literal, which has no Arrow representation).
pub fn to_datafusion_expr(expr: &Expression, input_schema: &StructType) -> DeltaResult<Expr> {
    match expr {
        // === 1. Natural type: self-describing, lowered here ===
        Expression::Literal(scalar) => Ok(lit(kernel_to_df_scalar(scalar)?)),
        Expression::Column(name) => column_to_expr(name, input_schema),
        Expression::Binary(binary) => binary_to_expr(binary, input_schema),
        Expression::Variadic(variadic) => variadic_to_expr(variadic, input_schema),

        // === 2. Predicate: a boolean-valued predicate used where a value is expected ===
        // Wired up by the predicate-conversion branch, which supplies the `Predicate -> Expr`
        // converter.
        Expression::Predicate(_) => Err(Error::unsupported(
            "converting an embedded Predicate expression is not yet supported",
        )),

        // === 3. Typed: lowerable only against a target output schema ===
        // `Struct` needs the schema for its field *names*; `MapToStruct` and `StructPatch` need it
        // for the field *types* that drive their reshape. This untyped conversion carries none, so
        // all three defer to the typed `Project`-node compiler.
        Expression::Struct(_, _)
        | Expression::MapToStruct(_)
        | Expression::StructPatch(_) => Err(Error::unsupported(
            "converting schema-dependent expressions (Struct, MapToStruct, StructPatch) requires \
             a typed projection context",
        )),

        // === 4. ParseJson: self-typed, but no stock DataFusion lowering ===
        // Unlike the typed arms above, `ParseJson` carries its own output schema, so it needs no
        // external context. It is unsupported only because DataFusion core has no stock expression
        // for typed JSON parsing -- lowering it requires a custom scalar UDF mirroring kernel's
        // `parse_json` decoder, which is not yet wired up.
        Expression::ParseJson(_) => Err(Error::unsupported(
            "converting a ParseJson expression requires a custom JSON-parsing UDF, not yet wired up",
        )),

        // === 5. Terminal: no faithful untyped lowering ===
        Expression::Unary(u) => match u.op {
            // `ToJson` needs a UDF kernel has not wired up.
            UnaryExpressionOp::ToJson => Err(Error::unsupported(
                "converting the ToJson expression is not yet supported",
            )),
        },
        // Engine-defined; kernel understands it only through its trait methods.
        Expression::Opaque(_) => Err(Error::unsupported(
            "cannot convert an engine-defined Opaque expression",
        )),
        // No semantics to lower: kernel forbids interpreting an Unknown expression.
        Expression::Unknown(name) => Err(Error::unsupported(format!(
            "cannot convert Unknown expression {name:?}"
        ))),
    }
}

/// Lowers a column reference to a `get_field` chain rooted at the first path segment, e.g. `a.b.c`
/// becomes `get_field(get_field(col("a"), "b"), "c")`. A single-segment path is just the root
/// column.
///
/// The path is first resolved against `input_schema` so a dangling reference fails here rather than
/// later during DataFusion plan analysis. The resolved field is not otherwise used: the emitted
/// `col(..)`/`get_field(..)` chain is nameless and DataFusion re-resolves it against the upstream
/// schema.
///
/// # Errors
///
/// Returns an error for an empty path, a segment that names no field, or an intermediate segment
/// whose field is not a struct (all surfaced by [`StructType::field_at`]).
fn column_to_expr(name: &ColumnName, input_schema: &StructType) -> DeltaResult<Expr> {
    input_schema.field_at(name)?;
    let mut path = name.iter();
    let root = path
        .next()
        .ok_or_else(|| Error::generic("cannot convert an empty column reference"))?;
    let root = Expr::Column(Column::new_unqualified(root));
    Ok(path.fold(root, get_field))
}

/// Lowers an arithmetic binary expression (`Plus`/`Minus`/`Multiply`/`Divide`) to an
/// `Expr::BinaryExpr`. Comparison and `IN` operators are modeled as predicates, not expressions,
/// so they never reach this arm.
fn binary_to_expr(binary: &BinaryExpression, input_schema: &StructType) -> DeltaResult<Expr> {
    let op = match binary.op {
        BinaryExpressionOp::Plus => Operator::Plus,
        BinaryExpressionOp::Minus => Operator::Minus,
        BinaryExpressionOp::Multiply => Operator::Multiply,
        BinaryExpressionOp::Divide => Operator::Divide,
    };
    let left = to_datafusion_expr(&binary.left, input_schema)?;
    let right = to_datafusion_expr(&binary.right, input_schema)?;
    Ok(binary_expr(left, op, right))
}

/// Lowers a variadic expression: `Coalesce` to `coalesce(..)` and `Array` to `make_array(..)`,
/// each over the converted arguments.
fn variadic_to_expr(variadic: &VariadicExpression, input_schema: &StructType) -> DeltaResult<Expr> {
    let args = variadic
        .exprs
        .iter()
        .map(|e| to_datafusion_expr(e, input_schema))
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
        to_datafusion_expr(&expr, &test_schema())
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

    /// A column reference that does not resolve against the input schema fails at conversion time,
    /// not later during DataFusion analysis. Covers each `field_at` failure mode.
    #[rstest]
    #[case::empty(Expr_::Column(ColumnName::new(Vec::<String>::new())))]
    #[case::unknown_root(Expr_::column(["nope"]))]
    #[case::unknown_nested(Expr_::column(["a", "b", "missing"]))]
    #[case::descend_into_non_struct(Expr_::column(["x", "y"]))]
    fn unresolved_column_is_an_error(#[case] kernel: Expr_) {
        to_datafusion_expr(&kernel, &test_schema()).unwrap_err();
    }
}
