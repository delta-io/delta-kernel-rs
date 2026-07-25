//! Conversion from a kernel [`Expression`](KernelExpression) to a DataFusion [`Expr`](DFExpr).

use datafusion::common::{Column as DFColumn, ScalarValue as DFScalarValue};
use datafusion::functions::core::expr_fn::{
    coalesce, get_field, get_field_path, named_struct, nullif,
};
use datafusion::functions_nested::expr_fn::make_array;
use datafusion::logical_expr::{binary_expr, cast, lit, Case, Expr as DFExpr, Operator};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::expressions::{
    BinaryExpression, BinaryExpressionOp, ColumnName as KernelColumnName,
    Expression as KernelExpression, ExpressionRef, ExpressionStructPatch, MapToStructExpression,
    UnaryExpressionOp, VariadicExpression, VariadicExpressionOp,
};
use delta_kernel::schema::{DataType as KernelDataType, PrimitiveType, StructField, StructType};
use delta_kernel::{DeltaResult, Error};

use crate::predicate::to_df_predicate_expr;
use crate::scalar::to_df_scalar;

/// Converts a kernel [`Expression`](KernelExpression) into the equivalent DataFusion
/// [`Expr`](DFExpr), validating columns against `input_schema`. `output_type` is the caller's
/// declared result type when known; the container-shaped arms (`Struct`, `StructPatch`, and an
/// `Array` of structs) take from it the field names and per-child types a constructor needs but the
/// expression does not carry. Other arms ignore it, so callers without one pass `None`.
///
/// # Errors
/// Returns an error for a column that does not resolve against `input_schema`; a struct-shaped arm
/// without a struct `output_type` or with a mismatched field count; an `Array` arm whose
/// `output_type` is not an array; and [`Error::unsupported`] for arms with no DataFusion equivalent
/// (see the `TODO`s below).
pub fn to_df_expr(
    expr: &KernelExpression,
    input_schema: &StructType,
    output_type: Option<&KernelDataType>,
) -> DeltaResult<DFExpr> {
    match expr {
        KernelExpression::Literal(scalar) => Ok(lit(to_df_scalar(scalar)?)),
        KernelExpression::Column(name) => column_to_df_expr(name, input_schema),
        KernelExpression::Binary(binary) => binary_expr_to_df_expr(binary, input_schema),
        KernelExpression::Variadic(variadic) => {
            variadic_to_df_expr(variadic, input_schema, output_type)
        }
        KernelExpression::Predicate(pred) => to_df_predicate_expr(pred, input_schema),
        KernelExpression::Struct(fields, nullability) => {
            struct_to_df_expr(fields, nullability.as_ref(), input_schema, output_type)
        }
        KernelExpression::StructPatch(patch) => {
            struct_patch_to_df_expr(patch, input_schema, output_type)
        }
        KernelExpression::MapToStruct(map_to_struct) => {
            map_to_struct_to_df_expr(map_to_struct, input_schema, output_type)
        }

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
    let left = to_df_expr(&binary.left, input_schema, None)?;
    let right = to_df_expr(&binary.right, input_schema, None)?;
    Ok(binary_expr(left, op, right))
}

/// Lowers a variadic expression: `Coalesce` to `coalesce(..)` and `Array` to `make_array(..)`, each
/// over the converted arguments. Coalesce is type-preserving, so it forwards `output_type` to each
/// argument (every branch produces the same type). Array is type-wrapping: a known `Array<E>`
/// target is peeled to `E` and threaded to each element (so an array of structs still gets its
/// element schema); an unknown target leaves elements untyped.
fn variadic_to_df_expr(
    variadic: &VariadicExpression,
    input_schema: &StructType,
    output_type: Option<&KernelDataType>,
) -> DeltaResult<DFExpr> {
    let arg_output_type = match variadic.op {
        VariadicExpressionOp::Coalesce => output_type,
        VariadicExpressionOp::Array => match output_type {
            Some(KernelDataType::Array(arr)) => Some(arr.element_type()),
            Some(other) => {
                return Err(Error::unsupported(format!(
                    "converting an Array expression requires an array output type, got {other:?}"
                )))
            }
            None => None,
        },
    };
    let args: DeltaResult<Vec<DFExpr>> = variadic
        .exprs
        .iter()
        .map(|e| to_df_expr(e, input_schema, arg_output_type))
        .collect();
    match variadic.op {
        VariadicExpressionOp::Coalesce => Ok(coalesce(args?)),
        VariadicExpressionOp::Array => Ok(make_array(args?)),
    }
}

/// Extracts the target struct type for a struct-shaped arm from the caller's `output_type`,
/// erroring if it is absent or not a [`KernelDataType::Struct`].
fn require_struct_output<'a>(
    output_type: Option<&'a KernelDataType>,
    arm: &str,
) -> DeltaResult<&'a StructType> {
    match output_type {
        Some(KernelDataType::Struct(schema)) => Ok(schema),
        Some(other) => Err(Error::unsupported(format!(
            "converting a {arm} expression requires a struct output type, got {other:?}"
        ))),
        None => Err(Error::unsupported(format!(
            "converting a {arm} expression requires a struct output type"
        ))),
    }
}

/// `CASE WHEN guard THEN body ELSE NULL END`: nulls the whole struct where `guard` is not true,
/// matching kernel's row-level struct-null mask. The else is an untyped NULL so CASE coercion
/// promotes it to `body`'s (all-nullable) struct type rather than forcing a nullability match.
fn struct_null_when_not(guard: DFExpr, body: DFExpr) -> DFExpr {
    DFExpr::Case(Case::new(
        None,
        vec![(Box::new(guard), Box::new(body))],
        Some(Box::new(lit(DFScalarValue::Null))),
    ))
}

/// Lowers a struct constructor to `named_struct(..)`, taking field names and per-child target types
/// from `output_type`. An optional nullability predicate nulls the whole struct where it is not
/// true.
fn struct_to_df_expr(
    fields: &[ExpressionRef],
    nullability: Option<&ExpressionRef>,
    input_schema: &StructType,
    output_type: Option<&KernelDataType>,
) -> DeltaResult<DFExpr> {
    let target = require_struct_output(output_type, "Struct")?;
    if fields.len() != target.num_fields() {
        return Err(Error::generic(format!(
            "Struct expression field count mismatch: {} fields in expression but {} in schema",
            fields.len(),
            target.num_fields()
        )));
    }
    // `named_struct` takes one flat arg list of alternating names and values:
    // `[name1, value1, name2, value2, ...]`, hence two args per field.
    let mut args = Vec::with_capacity(fields.len() * 2);
    for (child, field) in fields.iter().zip(target.fields()) {
        args.push(lit(field.name().to_string()));
        args.push(to_df_expr(child, input_schema, Some(field.data_type()))?);
    }
    let body = named_struct(args);
    let Some(pred) = nullability else {
        return Ok(body);
    };
    let guard = to_df_expr(pred, input_schema, None)?;
    Ok(struct_null_when_not(guard, body))
}

/// Lowers a struct patch (a sparse edit of an input struct) to a `named_struct(..)` rebuild,
/// drawing output field names and types positionally from `output_type`. Walks the evaluator's
/// emission order: prepends, each input field (passed through unless dropped/replaced, then its
/// insertions), appends. A nested patch (`input_path` set) nulls the output where the source struct
/// row is null, via `<input_path> IS NOT NULL` -- matching the evaluator, which clones the source
/// struct's null buffer.
fn struct_patch_to_df_expr(
    patch: &ExpressionStructPatch,
    input_schema: &StructType,
    output_type: Option<&KernelDataType>,
) -> DeltaResult<DFExpr> {
    let target = require_struct_output(output_type, "StructPatch")?;

    // A patch targets either the whole input struct (`input_path` is `None`), whose fields are the
    // top-level columns, or the nested struct at that path, whose fields are reached through it.
    let (mut source_struct, mut source_expr) = (input_schema, None);
    if let Some(path) = patch.input_path() {
        let KernelDataType::Struct(nested) = input_schema.field_at(path)?.data_type() else {
            return Err(Error::generic(format!(
                "StructPatch input_path '{path}' does not resolve to a struct"
            )));
        };
        let source = column_to_df_expr(path, input_schema)?;
        (source_struct, source_expr) = (nested.as_ref(), Some(source));
    }

    // Append `[name, value]` pairs in the evaluator's emission order, consuming one output field
    // per appended value so each value is lowered against the type it lands in.
    let mut output_fields = target.fields();
    let mut args = Vec::with_capacity(target.num_fields() * 2);

    // Both closures need the shared `output_fields` cursor, so it is threaded as a parameter rather
    // than captured.
    let append_field_with_converted_expr =
        |args: &mut Vec<DFExpr>,
         output_fields: &mut dyn Iterator<Item = &StructField>,
         expr: &KernelExpression|
         -> DeltaResult<()> {
            let field = output_fields.next().ok_or_else(|| {
                Error::generic("StructPatch produced more fields than the output schema has")
            })?;
            let value = to_df_expr(expr, input_schema, Some(field.data_type()))?;
            args.push(lit(field.name().to_string()));
            args.push(value);
            Ok(())
        };
    let append_field_with_existing_col = |args: &mut Vec<DFExpr>,
                                          output_fields: &mut dyn Iterator<Item = &StructField>,
                                          name: &str|
     -> DeltaResult<()> {
        let field = output_fields.next().ok_or_else(|| {
            Error::generic("StructPatch produced more fields than the output schema has")
        })?;
        let value = match &source_expr {
            Some(base) => get_field(base.clone(), name.to_string()),
            None => DFExpr::Column(DFColumn::new_unqualified(name)),
        };
        args.push(lit(field.name().to_string()));
        args.push(value);
        Ok(())
    };

    for expr in &patch.prepended_fields {
        append_field_with_converted_expr(&mut args, &mut output_fields, expr)?;
    }

    let mut used_field_patches = 0usize;
    for input_field in source_struct.fields() {
        let name = input_field.name();
        let field_patch = patch.field_patches.get(name);

        if field_patch.is_none_or(|fp| fp.keep_input) {
            append_field_with_existing_col(&mut args, &mut output_fields, name)?;
        }

        let Some(field_patch) = field_patch else {
            continue;
        };
        for expr in &field_patch.insertions {
            append_field_with_converted_expr(&mut args, &mut output_fields, expr)?;
        }
        used_field_patches += 1;
    }

    let required = patch
        .field_patches
        .values()
        .filter(|fp| !fp.optional)
        .count();
    if used_field_patches < required {
        return Err(Error::generic(
            "StructPatch has non-optional field patches that reference missing input fields",
        ));
    }

    for expr in &patch.appended_fields {
        append_field_with_converted_expr(&mut args, &mut output_fields, expr)?;
    }

    if output_fields.next().is_some() {
        return Err(Error::generic(
            "StructPatch produced fewer fields than the output schema has",
        ));
    }

    let body = named_struct(args);
    let Some(base) = source_expr else {
        return Ok(body);
    };
    Ok(struct_null_when_not(base.is_not_null(), body))
}

/// Lowers a `MapToStruct` (reshape a `Map<String, String>` into a struct by parsing each value into
/// its target field type) to a DataFusion `named_struct(..)` rebuild. Field names and per-field
/// types come from `output_type`, which must be a struct holding only primitive fields (matching
/// the kernel evaluator, which supports only primitive targets).
///
/// Each field extracts its value with `cast(get_field(map, name), T)`. For a numeric or temporal
/// type the raw value is first wrapped in `nullif(.., '')`, mapping an empty string to null before
/// the cast, so an empty string becomes null (kernel's `empty_string_partition_cast`) while an
/// unparseable value fails the cast (kernel's hard parse error). String and Binary keep the raw
/// value (empty is a valid empty string / empty bytes). A missing key or null value is already null
/// via `get_field`. The whole struct is nulled where the input map row is null, via `<map> IS NOT
/// NULL`.
///
/// KNOWN DIVERGENCES from the kernel parser, all confined to malformed or non-spec-compliant input
/// (spec-compliant writers never emit any of these):
/// - Duplicate keys: `get_field` takes the leftmost entry, the kernel evaluator the rightmost.
/// - Boolean: arrow's cast also accepts `"yes"`/`"no"`/`"on"`/`"off"`/`"t"`/`"f"`/`"1"`/`"0"`,
///   while kernel accepts only `"true"`/`"false"`.
/// - Decimal: arrow's cast silently rescales/rounds to the target scale, while kernel requires the
///   value's scale to match the target's exactly (and hard-errors otherwise).
///
/// # Errors
///
/// Returns an error when `output_type` is absent, not a struct, or has a non-primitive field, or
/// from lowering the map expression.
fn map_to_struct_to_df_expr(
    map_to_struct: &MapToStructExpression,
    input_schema: &StructType,
    output_type: Option<&KernelDataType>,
) -> DeltaResult<DFExpr> {
    let target = require_struct_output(output_type, "MapToStruct")?;
    let map = to_df_expr(&map_to_struct.map_expr, input_schema, None)?;

    let mut args = Vec::with_capacity(target.num_fields() * 2);
    for field in target.fields() {
        let KernelDataType::Primitive(prim) = field.data_type() else {
            return Err(Error::unsupported(format!(
                "MapToStruct only supports primitive target types, but field '{}' is {:?}",
                field.name(),
                field.data_type()
            )));
        };
        let raw = get_field(map.clone(), field.name().to_string());
        let value = match prim {
            PrimitiveType::String | PrimitiveType::Binary => raw,
            _ => nullif(raw, lit("")),
        };
        let arrow_type = field
            .data_type()
            .try_into_arrow()
            .map_err(Error::generic_err)?;
        args.push(lit(field.name().to_string()));
        args.push(cast(value, arrow_type));
    }

    Ok(struct_null_when_not(map.is_not_null(), named_struct(args)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Int32Array, MapBuilder,
        RecordBatch, StringArray, StringBuilder, StructArray, TimestampMicrosecondArray,
    };
    use datafusion::arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};
    use datafusion::common::DFSchema;
    use datafusion::execution::context::SessionContext;
    use delta_kernel::expressions::{
        column_expr, Expression as KernelExpr, ExpressionStructPatch, ExpressionStructPatchBuilder,
    };
    use delta_kernel::schema::{ArrayType, DataType, MapType, StructField, StructType};
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
        to_df_expr(&expr, &test_schema(), None).unwrap().to_string()
    }

    /// Lowers against [`test_schema`] targeting `output_type` and renders as a `Display` string.
    fn lower_typed(expr: KernelExpr, output_type: DataType) -> String {
        to_df_expr(&expr, &test_schema(), Some(&output_type))
            .unwrap()
            .to_string()
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
    fn nested_arithmetic_lowers_to_operator_tree(
        #[case] kernel: KernelExpr,
        #[case] expected: &str,
    ) {
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

    /// An array of structs peels the element type off the `Array<Struct>` target and threads the
    /// struct schema to each element, so the struct children get their field names.
    #[test]
    fn array_of_struct_threads_element_schema_to_each_element() {
        let element = KernelExpr::struct_from([column_expr!("b"), KernelExpr::literal(1i64)]);
        let kernel = KernelExpr::array([element]);
        let target: DataType = ArrayType::new(pq_struct(), true).into();
        assert_eq!(
            lower_typed(kernel, target),
            "make_array(named_struct(Utf8(\"p\"), b, Utf8(\"q\"), Int64(1)))"
        );
    }

    /// Nested `Array<Array<Struct>>`: the element type is peeled at each array level until the
    /// struct schema reaches the leaf struct element.
    #[test]
    fn nested_array_of_array_peels_element_type_at_each_level() {
        let inner = KernelExpr::array([KernelExpr::struct_from([column_expr!("b")])]);
        let kernel = KernelExpr::array([inner]);
        let leaf = StructType::try_new([StructField::nullable("p", DataType::LONG)]).unwrap();
        let target: DataType = ArrayType::new(ArrayType::new(leaf, true), true).into();
        assert_eq!(
            lower_typed(kernel, target),
            "make_array(make_array(named_struct(Utf8(\"p\"), b)))"
        );
    }

    /// An `Array` arm errors when it cannot resolve its element type: no target at all leaves a
    /// struct element without field names (same as a bare `Struct`), and a non-array target has no
    /// element type to peel.
    #[rstest]
    #[case::struct_element_without_target(
        KernelExpr::array([KernelExpr::struct_from([column_expr!("b")])]),
        None
    )]
    #[case::non_array_target(KernelExpr::array([KernelExpr::literal(1i64)]), Some(DataType::LONG))]
    fn array_with_unresolvable_element_type_is_an_error(
        #[case] kernel: KernelExpr,
        #[case] output_type: Option<DataType>,
    ) {
        to_df_expr(&kernel, &test_schema(), output_type.as_ref()).unwrap_err();
    }

    #[test]
    fn embedded_predicate_delegates_to_predicate_converter() {
        let kernel = KernelExpr::Predicate(Box::new(column_expr!("b").is_null()));
        assert_eq!(lower(kernel), "b IS NULL");
    }

    /// A column reference that does not resolve against the input schema fails at conversion time,
    /// not later during DataFusion analysis. Covers each `field_at` failure mode.
    #[rstest]
    #[case::empty(KernelExpr::Column(KernelColumnName::default()))]
    #[case::unknown_root(KernelExpr::column(["nope"]))]
    #[case::unknown_nested(KernelExpr::column(["a", "b", "missing"]))]
    #[case::descend_into_non_struct(KernelExpr::column(["x", "y"]))]
    fn unresolved_column_is_an_error(#[case] kernel: KernelExpr) {
        to_df_expr(&kernel, &test_schema(), None).unwrap_err();
    }

    // === Struct ===

    /// A two-field target struct `{ p: long, q: long }` for struct/patch tests.
    fn pq_struct() -> StructType {
        StructType::try_new([
            StructField::nullable("p", DataType::LONG),
            StructField::nullable("q", DataType::LONG),
        ])
        .unwrap()
    }

    #[test]
    fn struct_lowers_to_named_struct_with_target_names() {
        let kernel = KernelExpr::struct_from([column_expr!("b"), KernelExpr::literal(1i64)]);
        assert_eq!(
            lower_typed(kernel, pq_struct().into()),
            "named_struct(Utf8(\"p\"), b, Utf8(\"q\"), Int64(1))"
        );
    }

    #[test]
    fn nested_struct_recurses_with_child_target_names() {
        let inner = KernelExpr::struct_from([column_expr!("b"), KernelExpr::literal(1i64)]);
        let kernel = KernelExpr::struct_from([inner]);
        let target = StructType::try_new([StructField::nullable("outer", pq_struct())]).unwrap();
        assert_eq!(
            lower_typed(kernel, target.into()),
            "named_struct(Utf8(\"outer\"), named_struct(Utf8(\"p\"), b, Utf8(\"q\"), Int64(1)))"
        );
    }

    #[test]
    fn struct_with_nullability_wraps_in_case() {
        let kernel = KernelExpr::struct_with_nullability_from(
            [column_expr!("b"), KernelExpr::literal(1i64)],
            KernelExpr::Predicate(Box::new(column_expr!("x").is_not_null())),
        );
        // Kernel models IS NOT NULL as Not(IsNull), so the guard renders as "NOT x IS NULL".
        let rendered = lower_typed(kernel, pq_struct().into());
        assert!(
            rendered.starts_with("CASE WHEN NOT x IS NULL THEN named_struct("),
            "{rendered}"
        );
        assert!(rendered.ends_with("END"), "{rendered}");
    }

    #[test]
    fn struct_without_target_is_unsupported() {
        let kernel = KernelExpr::struct_from([column_expr!("b")]);
        to_df_expr(&kernel, &test_schema(), None).unwrap_err();
    }

    #[test]
    fn struct_arity_mismatch_is_an_error() {
        let kernel = KernelExpr::struct_from([column_expr!("b"), KernelExpr::literal(1i64)]);
        let target: DataType = StructType::try_new([StructField::nullable("p", DataType::LONG)])
            .unwrap()
            .into();
        to_df_expr(&kernel, &test_schema(), Some(&target)).unwrap_err();
    }

    // === Struct patch ===

    /// Lowers a struct patch against `input`, targeting `output_schema`.
    fn lower_patch(
        patch: ExpressionStructPatch,
        input: &StructType,
        output_schema: &StructType,
    ) -> String {
        let expr = KernelExpr::struct_patch(patch).unwrap();
        let output_type: DataType = output_schema.clone().into();
        to_df_expr(&expr, input, Some(&output_type))
            .unwrap()
            .to_string()
    }

    /// Input struct `{ a: long, b: long }` for patch tests: the whole input schema for a top-level
    /// patch, or the nested source struct for a nested one.
    fn ab_schema() -> StructType {
        StructType::try_new([
            StructField::nullable("a", DataType::LONG),
            StructField::nullable("b", DataType::LONG),
        ])
        .unwrap()
    }

    /// Asserts `res` is an error whose message contains `message`.
    #[track_caller]
    fn assert_error_message<T>(res: DeltaResult<T>, message: &str) {
        let error = res.err().expect("expected an error").to_string();
        assert!(error.contains(message), "{error}");
    }

    #[test]
    fn empty_top_level_patch_passes_all_fields_through() {
        let patch = ExpressionStructPatchBuilder::new().build().unwrap();
        assert_eq!(
            lower_patch(patch, &ab_schema(), &pq_struct()),
            "named_struct(Utf8(\"p\"), a, Utf8(\"q\"), b)"
        );
    }

    #[test]
    fn top_level_patch_replace_puts_expr_in_field_slot() {
        let patch = ExpressionStructPatchBuilder::new()
            .replace("a", KernelExpr::literal(7i64))
            .build()
            .unwrap();
        assert_eq!(
            lower_patch(patch, &ab_schema(), &pq_struct()),
            "named_struct(Utf8(\"p\"), Int64(7), Utf8(\"q\"), b)"
        );
    }

    #[test]
    fn top_level_patch_drop_removes_field() {
        let patch = ExpressionStructPatchBuilder::new()
            .drop("a")
            .build()
            .unwrap();
        let target = StructType::try_new([StructField::nullable("q", DataType::LONG)]).unwrap();
        assert_eq!(
            lower_patch(patch, &ab_schema(), &target),
            "named_struct(Utf8(\"q\"), b)"
        );
    }

    #[test]
    fn top_level_patch_prepend_and_append() {
        let patch = ExpressionStructPatchBuilder::new()
            .prepend(KernelExpr::literal(0i64))
            .append(KernelExpr::literal(9i64))
            .build()
            .unwrap();
        let target = StructType::try_new([
            StructField::nullable("first", DataType::LONG),
            StructField::nullable("a", DataType::LONG),
            StructField::nullable("b", DataType::LONG),
            StructField::nullable("last", DataType::LONG),
        ])
        .unwrap();
        assert_eq!(
            lower_patch(patch, &ab_schema(), &target),
            "named_struct(Utf8(\"first\"), Int64(0), Utf8(\"a\"), a, Utf8(\"b\"), b, \
             Utf8(\"last\"), Int64(9))"
        );
    }

    #[test]
    fn top_level_patch_insert_after_field() {
        let patch = ExpressionStructPatchBuilder::new()
            .insert_after("a", KernelExpr::literal(5i64))
            .build()
            .unwrap();
        let target = StructType::try_new([
            StructField::nullable("a", DataType::LONG),
            StructField::nullable("inserted", DataType::LONG),
            StructField::nullable("b", DataType::LONG),
        ])
        .unwrap();
        assert_eq!(
            lower_patch(patch, &ab_schema(), &target),
            "named_struct(Utf8(\"a\"), a, Utf8(\"inserted\"), Int64(5), Utf8(\"b\"), b)"
        );
    }

    #[test]
    fn nested_patch_wraps_in_null_guard_case() {
        // Input schema: { s: { a: long, b: long } }. Patch replaces s.a with a literal.
        let input = StructType::try_new([StructField::nullable("s", ab_schema())]).unwrap();
        let patch = ExpressionStructPatchBuilder::new_nested(["s"])
            .replace("a", KernelExpr::literal(7i64))
            .build()
            .unwrap();
        assert_eq!(
            lower_patch(patch, &input, &pq_struct()),
            "CASE WHEN s IS NOT NULL THEN named_struct(Utf8(\"p\"), Int64(7), Utf8(\"q\"), \
             get_field(s, Utf8(\"b\"))) ELSE NULL END"
        );
    }

    #[test]
    fn patch_too_many_output_fields_is_an_error() {
        // Empty patch passes 2 fields; target declares 3.
        let patch = ExpressionStructPatchBuilder::new().build().unwrap();
        let target: DataType = StructType::try_new([
            StructField::nullable("p", DataType::LONG),
            StructField::nullable("q", DataType::LONG),
            StructField::nullable("r", DataType::LONG),
        ])
        .unwrap()
        .into();
        let expr = KernelExpr::struct_patch(patch).unwrap();
        assert_error_message(
            to_df_expr(&expr, &ab_schema(), Some(&target)),
            "StructPatch produced fewer fields than the output schema has",
        );
    }

    #[test]
    fn patch_too_few_output_fields_is_an_error() {
        // Empty patch passes 2 fields; target declares 1.
        let patch = ExpressionStructPatchBuilder::new().build().unwrap();
        let target: DataType = StructType::try_new([StructField::nullable("p", DataType::LONG)])
            .unwrap()
            .into();
        let expr = KernelExpr::struct_patch(patch).unwrap();
        assert_error_message(
            to_df_expr(&expr, &ab_schema(), Some(&target)),
            "StructPatch produced more fields than the output schema has",
        );
    }

    #[test]
    fn patch_without_target_is_unsupported() {
        let patch = ExpressionStructPatchBuilder::new().build().unwrap();
        let expr = KernelExpr::struct_patch(patch).unwrap();
        assert_error_message(
            to_df_expr(&expr, &ab_schema(), None),
            "converting a StructPatch expression requires a struct output type",
        );
    }

    #[test]
    fn required_patch_on_missing_field_is_an_error() {
        let patch = ExpressionStructPatchBuilder::new()
            .replace("nonexistent", KernelExpr::literal(1i64))
            .build()
            .unwrap();
        let expr = KernelExpr::struct_patch(patch).unwrap();
        let target: DataType = pq_struct().into();
        assert_error_message(
            to_df_expr(&expr, &ab_schema(), Some(&target)),
            "StructPatch has non-optional field patches that reference missing input fields",
        );
    }

    #[test]
    fn optional_patch_on_missing_field_is_tolerated() {
        // An optional drop on a missing field is silently ignored.
        let patch = ExpressionStructPatchBuilder::new()
            .drop_if_exists("nonexistent")
            .build()
            .unwrap();
        assert_eq!(
            lower_patch(patch, &ab_schema(), &pq_struct()),
            "named_struct(Utf8(\"p\"), a, Utf8(\"q\"), b)"
        );
    }

    /// A struct target is re-derived and threaded at every nesting level: a `StructPatch` whose
    /// appended field `g` is a `Struct` whose field `h` is a `Struct` whose `leaf` is a column.
    /// Each level pulls its child's sub-schema from its own field type, so names land correctly all
    /// the way down (`g` from the patch target, `h` from g's sub-schema, `leaf` from h's).
    #[test]
    fn nested_struct_targets_are_rederived_at_each_level() {
        let deepest = KernelExpr::struct_from([column_expr!("a")]); // { leaf: a }
        let middle = KernelExpr::struct_from([deepest]); // { h: { leaf } }
        let patch = ExpressionStructPatchBuilder::new()
            .append(middle)
            .build()
            .unwrap();
        let target = StructType::try_new([
            StructField::nullable("a", DataType::LONG),
            StructField::nullable("b", DataType::LONG),
            StructField::nullable(
                "g",
                StructType::try_new([StructField::nullable(
                    "h",
                    StructType::try_new([StructField::nullable("leaf", DataType::LONG)]).unwrap(),
                )])
                .unwrap(),
            ),
        ])
        .unwrap();
        assert_eq!(
            lower_patch(patch, &ab_schema(), &target),
            "named_struct(Utf8(\"a\"), a, Utf8(\"b\"), b, Utf8(\"g\"), \
             named_struct(Utf8(\"h\"), named_struct(Utf8(\"leaf\"), a)))"
        );
    }

    // === Execution ===

    /// Builds a physical expr against `batch`'s schema (running DataFusion's simplification + type
    /// coercion, which Display-string assertions cannot exercise), evaluates it, returns the array.
    fn eval_against(expr: DFExpr, batch: &RecordBatch) -> ArrayRef {
        let df_schema = DFSchema::try_from(batch.schema()).unwrap();
        let physical = SessionContext::new()
            .create_physical_expr(expr, &df_schema)
            .unwrap();
        physical
            .evaluate(batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap()
    }

    // === MapToStruct ===

    /// Input schema for map tests: `{ pv: map<string, string> }`.
    fn pv_map_schema() -> StructType {
        StructType::try_new([StructField::nullable(
            "pv",
            MapType::new(DataType::STRING, DataType::STRING, true),
        )])
        .unwrap()
    }

    /// A `{ region: string, id: integer }` target struct for map tests.
    fn region_id_struct() -> StructType {
        StructType::try_new([
            StructField::nullable("region", DataType::STRING),
            StructField::nullable("id", DataType::INTEGER),
        ])
        .unwrap()
    }

    /// Builds a one-column `pv: map<string, string>` batch; each row is an optional list of
    /// entries (`None` = a null map row).
    fn map_batch(rows: Vec<Option<Vec<(&str, &str)>>>) -> RecordBatch {
        let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        for row in rows {
            match row {
                Some(entries) => {
                    for (k, v) in entries {
                        builder.keys().append_value(k);
                        builder.values().append_value(v);
                    }
                    builder.append(true).unwrap();
                }
                None => builder.append(false).unwrap(),
            }
        }
        let map = builder.finish();
        let schema = ArrowSchema::new(vec![ArrowField::new("pv", map.data_type().clone(), true)]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(map)]).unwrap()
    }

    /// Lowers a `MapToStruct` over `pv` targeting `output_schema`, evaluates it against `batch`,
    /// and returns the resulting struct array.
    fn eval_map_to_struct(output_schema: &StructType, batch: &RecordBatch) -> StructArray {
        let kernel = KernelExpr::map_to_struct(column_expr!("pv"));
        let output_type: DataType = output_schema.clone().into();
        let expr = to_df_expr(&kernel, &pv_map_schema(), Some(&output_type)).unwrap();
        eval_against(expr, batch)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .clone()
    }

    #[test]
    fn map_to_struct_lowers_to_named_struct_over_get_field() {
        let kernel = KernelExpr::map_to_struct(column_expr!("pv"));
        let target: DataType = region_id_struct().into();
        let rendered = to_df_expr(&kernel, &pv_map_schema(), Some(&target))
            .unwrap()
            .to_string();
        // Null-map guard wraps the rebuild; text field is a bare cast, non-text goes through
        // nullif.
        assert!(
            rendered.starts_with("CASE WHEN pv IS NOT NULL THEN named_struct("),
            "{rendered}"
        );
        assert!(
            rendered.contains("CAST(get_field(pv, Utf8(\"region\")) AS Utf8)"),
            "{rendered}"
        );
        assert!(
            rendered.contains("CAST(nullif(get_field(pv, Utf8(\"id\")), Utf8(\"\")) AS Int32)"),
            "{rendered}"
        );
        assert!(rendered.ends_with("END"), "{rendered}");
    }

    #[test]
    fn map_to_struct_without_target_is_unsupported() {
        let kernel = KernelExpr::map_to_struct(column_expr!("pv"));
        to_df_expr(&kernel, &pv_map_schema(), None).unwrap_err();
    }

    #[test]
    fn map_to_struct_non_primitive_field_is_unsupported() {
        let target: DataType = StructType::try_new([StructField::nullable("nested", pq_struct())])
            .unwrap()
            .into();
        let kernel = KernelExpr::map_to_struct(column_expr!("pv"));
        to_df_expr(&kernel, &pv_map_schema(), Some(&target)).unwrap_err();
    }

    #[test]
    fn map_to_struct_parses_present_values_and_nulls_missing_keys() {
        // Row 0 has both keys; row 1 is missing `id`.
        let batch = map_batch(vec![
            Some(vec![("region", "us"), ("id", "7")]),
            Some(vec![("region", "eu")]),
        ]);
        let structs = eval_map_to_struct(&region_id_struct(), &batch);
        let region = structs
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let id = structs
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(region.value(0), "us");
        assert_eq!(id.value(0), 7);
        assert_eq!(region.value(1), "eu");
        assert!(id.is_null(1), "missing key -> null");
    }

    #[rstest]
    // Empty string is a value for String/Binary (empty) but null for a numeric type.
    #[case::string_keeps_empty(DataType::STRING, true)]
    #[case::binary_keeps_empty(DataType::BINARY, true)]
    #[case::integer_nulls_empty(DataType::INTEGER, false)]
    fn map_to_struct_empty_string_cast_semantics(
        #[case] field_type: DataType,
        #[case] expect_valid: bool,
    ) {
        let target = StructType::try_new([StructField::nullable("f", field_type)]).unwrap();
        let batch = map_batch(vec![Some(vec![("f", "")])]);
        let structs = eval_map_to_struct(&target, &batch);
        assert_eq!(structs.column(0).is_valid(0), expect_valid);
    }

    #[test]
    fn map_to_struct_unparseable_value_is_a_hard_error() {
        let target: DataType =
            StructType::try_new([StructField::nullable("id", DataType::INTEGER)])
                .unwrap()
                .into();
        let batch = map_batch(vec![Some(vec![("id", "not-a-number")])]);
        let kernel = KernelExpr::map_to_struct(column_expr!("pv"));
        let expr = to_df_expr(&kernel, &pv_map_schema(), Some(&target)).unwrap();
        let df_schema = DFSchema::try_from(batch.schema()).unwrap();
        let physical = SessionContext::new()
            .create_physical_expr(expr, &df_schema)
            .unwrap();
        physical.evaluate(&batch).unwrap_err();
    }

    #[test]
    fn map_to_struct_null_map_row_yields_null_struct() {
        // Row 1's map is null; even with non-nullable-looking targets the struct row is null.
        let batch = map_batch(vec![Some(vec![("region", "us"), ("id", "1")]), None]);
        let structs = eval_map_to_struct(&region_id_struct(), &batch);
        assert!(structs.is_valid(0));
        assert!(structs.is_null(1), "null map row -> null struct");
    }

    /// Characterizes the documented divergence from kernel: `get_field` takes the leftmost
    /// duplicate entry, while the kernel evaluator takes the rightmost.
    #[test]
    fn map_to_struct_duplicate_keys_diverge_taking_leftmost() {
        let target = StructType::try_new([StructField::nullable("id", DataType::INTEGER)]).unwrap();
        let batch = map_batch(vec![Some(vec![("id", "1"), ("id", "2")])]);
        let structs = eval_map_to_struct(&target, &batch);
        let id = structs
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id.value(0), 1);
    }

    #[test]
    fn map_to_struct_present_empty_map_yields_present_struct_with_null_fields() {
        // A present-but-empty map `{}` is a non-null row that matches no keys: the struct stays
        // present with all fields null (only a null map row nulls the whole struct).
        let batch = map_batch(vec![Some(vec![])]);
        let structs = eval_map_to_struct(&region_id_struct(), &batch);
        assert!(structs.is_valid(0), "present empty map -> present struct");
        assert!(structs.column(0).is_null(0));
        assert!(structs.column(1).is_null(0));
    }

    /// The temporal casts are load-bearing: kernel parses partition timestamps/dates through the
    /// same arrow parsers, so `cast` must reproduce the UTC-normalized instant and epoch day.
    #[test]
    fn map_to_struct_parses_date_and_utc_normalized_timestamp() {
        let target = StructType::try_new([
            StructField::nullable("d", DataType::DATE),
            StructField::nullable("ts", DataType::TIMESTAMP),
        ])
        .unwrap();
        let batch = map_batch(vec![Some(vec![
            ("d", "2024-01-15"),
            ("ts", "2024-06-15T14:30:00+05:00"),
        ])]);
        let structs = eval_map_to_struct(&target, &batch);
        let d = structs
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        let ts = structs
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(d.value(0), 19737);
        assert_eq!(ts.value(0), 1_718_443_800_000_000);
    }

    fn eval_single_field(field_type: DataType, value: &str) -> ArrayRef {
        let target = StructType::try_new([StructField::nullable("f", field_type)]).unwrap();
        let batch = map_batch(vec![Some(vec![("f", value)])]);
        eval_map_to_struct(&target, &batch).column(0).clone()
    }

    /// Booleans parse for canonical values; the documented divergence is that arrow's cast also
    /// accepts non-canonical strings kernel rejects (e.g. `"yes"` -> true).
    #[rstest]
    #[case::t("true", true)]
    #[case::f("false", false)]
    #[case::diverge_yes("yes", true)]
    #[case::diverge_one("1", true)]
    fn map_to_struct_boolean_cast(#[case] value: &str, #[case] expected: bool) {
        let col = eval_single_field(DataType::BOOLEAN, value);
        let b = col.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(b.value(0), expected);
    }

    /// A matching-scale decimal parses; the documented divergence is that arrow's cast silently
    /// rescales a differently-scaled value (`"1.5"` -> `1.50`) where kernel hard-errors.
    #[test]
    fn map_to_struct_decimal_cast_rescales() {
        let col = eval_single_field(DataType::decimal(5, 2).unwrap(), "1.5");
        let d = col.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(d.value(0), 150); // 1.50 at scale 2
    }
}
