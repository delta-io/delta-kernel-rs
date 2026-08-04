//! Conversion from a kernel [`Expression`](KernelExpression) to a DataFusion [`Expr`](DFExpr).

use std::sync::Arc;

use datafusion::arrow::array::{new_null_array, ArrayRef, RecordBatch, StructArray};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
use datafusion::common::utils::take_function_args;
use datafusion::common::{Column as DFColumn, DataFusionError, ScalarValue as DFScalarValue};
use datafusion::functions::core::expr_fn::{
    coalesce, get_field, get_field_path, named_struct, nullif,
};
use datafusion::functions_nested::expr_fn::make_array;
use datafusion::logical_expr::{
    binary_expr, cast, lit, Case, ColumnarValue, Expr as DFExpr, Operator, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::parse_json;
use delta_kernel::expressions::{
    BinaryExpression, BinaryExpressionOp, ColumnName as KernelColumnName,
    Expression as KernelExpression, ExpressionRef, ExpressionStructPatch, MapToStructExpression,
    ParseJsonExpression, UnaryExpressionOp, VariadicExpression, VariadicExpressionOp,
};
use delta_kernel::schema::{
    DataType as KernelDataType, PrimitiveType, SchemaRef as KernelSchemaRef, StructField,
    StructType,
};
use delta_kernel::{DeltaResult, EngineData, Error};

use crate::predicate::to_df_predicate_expr;
use crate::scalar::to_df_scalar;

/// Converts `expr` into the equivalent DataFusion [`Expr`](DFExpr), resolving column references
/// against `input_schema`.
///
/// `output_type` supplies result type information that the expression itself does not carry.
/// `Struct` and `StructPatch` require a struct type for output field names and computed child
/// types. `Array` validates that a supplied type is an array and forwards its element type, while
/// `Coalesce` forwards the result type unchanged to every branch. Callers pass `None` when the
/// result type is unknown or the expression does not need it.
///
/// # Errors
/// Returns an error when a column cannot be resolved, a scalar cannot be converted, supplied type
/// information is incompatible with the expression, or a `StructPatch` is inconsistent with its
/// input or output schema. Returns [`Error::unsupported`] when no DataFusion equivalent is
/// implemented.
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
        KernelExpression::ParseJson(parse) => parse_json_to_df_expr(parse, input_schema),

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

/// Lowers a struct patch (a sparse edit of an input struct) to a `named_struct(..)` rebuild. Output
/// field names come positionally from `output_type`, whose corresponding field types are forwarded
/// when lowering computed values. Walks the evaluator's emission order: prepends, each input field
/// (passed through unless dropped/replaced, then its insertions), and appends. A nested patch
/// (`input_path` set) nulls the output where the source struct row is null, matching the
/// evaluator's preservation of the source struct's null buffer.
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

    // Should only count required field patches (excluding optional) for missing input fields
    // validation. An existing optional field can shadow a missing required field.
    let mut used_required_field_patches = 0usize;
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
        if !field_patch.optional {
            used_required_field_patches += 1;
        }
    }

    let required = patch
        .field_patches
        .values()
        .filter(|fp| !fp.optional)
        .count();
    if used_required_field_patches < required {
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
/// via [`get_field`]. The whole struct is nulled where the input map row is null, via `<map> IS NOT
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
            // An empty string is a value for these two (the empty string / empty bytes) and null
            // for every other type based on kernel.
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

/// Lowers a `ParseJson` (parse a JSON-string column into a struct) to a call of the
/// [`ParseJsonUdf`] scalar UDF, which delegates to kernel's own JSON parser. Unlike the
/// struct-shaped arms, `ParseJson` is self-typed -- it carries its target `output_schema` -- so it
/// takes no `output_type` and lowers its string operand untyped.
fn parse_json_to_df_expr(
    parse: &ParseJsonExpression,
    input_schema: &StructType,
) -> DeltaResult<DFExpr> {
    let json = to_df_expr(&parse.json_expr, input_schema, None)?;
    let udf = ScalarUDF::new_from_impl(ParseJsonUdf::try_new(parse.output_schema.clone())?);
    Ok(udf.call(vec![json]))
}

/// A DataFusion scalar UDF that parses a JSON-string column into a struct, delegating to kernel's
/// [`parse_json`] so the result is value-identical to the kernel evaluator by construction. Since a
/// [`ParseJsonExpression`] carries its own target schema, the schema is baked into the UDF instance
/// rather than passed as an argument.
///
/// The UDF reproduces the coarse malformed-JSON backstop the evaluator applies around
/// `parse_json`: on a whole-batch parse error it returns an all-null struct rather than failing.
/// (The finer per-cell null for failure-prone leaves -- Timestamp/Date/Decimal -- already lives
/// inside `parse_json`.)
#[derive(Debug, PartialEq, Eq)]
struct ParseJsonUdf {
    output_schema: KernelSchemaRef,
    return_type: ArrowDataType,
    signature: Signature,
}

/// DataFusion requires scalar UDF implementations to support equality and hashing so UDF calls
/// can participate in expression comparison and optimizations such as common-subexpression
/// elimination. The output schema captures this UDF's schema-dependent behavior; its signature is
/// otherwise identical for every instance.
impl std::hash::Hash for ParseJsonUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for field in self.output_schema.fields() {
            field.name().hash(state);
            field.data_type().to_string().hash(state);
        }
    }
}

impl ParseJsonUdf {
    fn try_new(output_schema: KernelSchemaRef) -> DeltaResult<Self> {
        let arrow_schema: ArrowSchema = output_schema
            .as_ref()
            .try_into_arrow()
            .map_err(Error::generic_err)?;
        Ok(Self {
            return_type: ArrowDataType::Struct(arrow_schema.fields().clone()),
            // JSON lives in a string column; accept the three arrow string layouts, mirroring
            // kernel's `parse_json_impl` (Utf8 / LargeUtf8 / Utf8View).
            signature: Signature::uniform(
                1,
                vec![
                    ArrowDataType::Utf8,
                    ArrowDataType::LargeUtf8,
                    ArrowDataType::Utf8View,
                ],
                Volatility::Immutable,
            ),
            output_schema,
        })
    }
}

impl ScalarUDFImpl for ParseJsonUdf {
    fn name(&self) -> &str {
        "kernel_parse_json"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[ArrowDataType]) -> Result<ArrowDataType, DataFusionError> {
        Ok(self.return_type.clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue, DataFusionError> {
        let num_rows = args.number_rows;
        let [json] = take_function_args(self.name(), args.args)?;
        let json = json.into_array(num_rows)?;

        // `parse_json` reads column 0 of an `EngineData`-wrapped batch; wrap the input to match.
        let batch = RecordBatch::try_from_iter([("json", json)])?;
        let input: Box<dyn EngineData> = Box::new(ArrowEngineData::from(batch));

        let parsed = match parse_json(input, self.output_schema.clone()) {
            Ok(data) => {
                let batch: RecordBatch = ArrowEngineData::try_from_engine_data(data)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                    .into();
                Arc::new(StructArray::from(batch)) as ArrayRef
            }
            // Coarse malformed-JSON backstop, matching the evaluator's ParseJson arm.
            Err(_) => new_null_array(&self.return_type, num_rows),
        };
        Ok(ColumnarValue::Array(parsed))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, AsArray, StringArray};
    use datafusion::arrow::datatypes::Int64Type;
    use datafusion::common::DFSchema;
    use datafusion::physical_expr::create_physical_expr;
    use datafusion::physical_expr::execution_props::ExecutionProps;
    use delta_kernel::expressions::{
        col, lit, Expression as KernelExpr, ExpressionStructPatch, ExpressionStructPatchBuilder,
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
    #[case::i32(lit(7i32), "Int32(7)")]
    #[case::i64(lit(42i64), "Int64(42)")]
    #[case::string(lit("abc"), "Utf8(\"abc\")")]
    #[case::boolean(lit(true), "Boolean(true)")]
    #[case::null(KernelExpr::null_literal(DataType::LONG), "Int64(NULL)")]
    fn literal_lowers_to_scalar(#[case] kernel: KernelExpr, #[case] expected: &str) {
        assert_eq!(lower(kernel), expected);
    }

    #[rstest]
    #[case::single(col!("a"), "a")]
    #[case::depth_2(col!("a.b"), "get_field(a, Utf8(\"b\"))")]
    #[case::depth_3(col!("a.b.c"), "get_field(a, Utf8(\"b\"), Utf8(\"c\"))")]
    fn column_lowers_to_nested_field_access(#[case] kernel: KernelExpr, #[case] expected: &str) {
        assert_eq!(lower(kernel), expected);
    }

    #[rstest]
    #[case::plus(col!("a") + lit(1i64), "a + Int64(1)")]
    #[case::minus(col!("a") - lit(1i64), "a - Int64(1)")]
    #[case::multiply(col!("a") * lit(2i64), "a * Int64(2)")]
    #[case::divide(col!("a") / lit(2i64), "a / Int64(2)")]
    fn arithmetic_binary_lowers_to_binary_expr(#[case] kernel: KernelExpr, #[case] expected: &str) {
        assert_eq!(lower(kernel), expected);
    }

    /// Nested arithmetic lowers to the matching operator tree.
    #[rstest]
    #[case::precedence_pins_grouping(
        (col!("x") + lit(1i64)) * (col!("b") - lit(2i64)),
        "(x + Int64(1)) * (b - Int64(2))"
    )]
    #[case::nested_field_and_all_ops(
        (col!("a.b.c") * lit(5i64) - (col!("b") + col!("x"))) / lit(20i64),
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
        KernelExpr::coalesce([col!("a"), col!("b"), lit(0i64)]),
        "coalesce(a, b, Int64(0))"
    )]
    #[case::array(
        KernelExpr::array([lit(1i64), lit(2i64)]),
        "make_array(Int64(1), Int64(2))"
    )]
    #[case::nested_coalesce(
        KernelExpr::coalesce([KernelExpr::coalesce([col!("a"), col!("b")]), col!("x")]),
        "coalesce(coalesce(a, b), x)"
    )]
    #[case::nested_array(
        KernelExpr::array([
            KernelExpr::array([lit(1i64), lit(2i64)]),
            KernelExpr::array([lit(3i64), lit(4i64)]),
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
        let element = KernelExpr::struct_from([col!("b"), lit(1i64)]);
        let kernel = KernelExpr::array([element]);
        let target: DataType = ArrayType::new(pq_output_schema(), true).into();
        assert_eq!(
            lower_typed(kernel, target),
            "make_array(named_struct(Utf8(\"p\"), b, Utf8(\"q\"), Int64(1)))"
        );
    }

    /// Nested `Array<Array<Struct>>`: the element type is peeled at each array level until the
    /// struct schema reaches the leaf struct element.
    #[test]
    fn nested_array_of_array_peels_element_type_at_each_level() {
        let inner = KernelExpr::array([KernelExpr::struct_from([col!("b")])]);
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
        KernelExpr::array([KernelExpr::struct_from([col!("b")])]),
        None
    )]
    #[case::non_array_target(KernelExpr::array([lit(1i64)]), Some(DataType::LONG))]
    fn array_with_unresolvable_element_type_is_an_error(
        #[case] kernel: KernelExpr,
        #[case] output_type: Option<DataType>,
    ) {
        to_df_expr(&kernel, &test_schema(), output_type.as_ref()).unwrap_err();
    }

    #[test]
    fn embedded_predicate_delegates_to_predicate_converter() {
        let kernel = KernelExpr::Predicate(Box::new(col!("b").is_null()));
        assert_eq!(lower(kernel), "b IS NULL");
    }

    /// A column reference that does not resolve against the input schema fails at conversion time,
    /// not later during DataFusion analysis. Covers each `field_at` failure mode.
    #[rstest]
    #[case::empty(KernelExpr::Column(KernelColumnName::default()))]
    #[case::unknown_root(col!("nope"))]
    #[case::unknown_nested(col!("a.b.missing"))]
    #[case::descend_into_non_struct(col!("x.y"))]
    fn unresolved_column_is_an_error(#[case] kernel: KernelExpr) {
        to_df_expr(&kernel, &test_schema(), None).unwrap_err();
    }

    // === Struct ===

    /// Output schema with names distinct from the input schema, proving names come from the target.
    fn pq_output_schema() -> StructType {
        StructType::try_new([
            StructField::nullable("p", DataType::LONG),
            StructField::nullable("q", DataType::LONG),
        ])
        .unwrap()
    }

    #[test]
    fn struct_lowers_to_named_struct_with_target_names() {
        let kernel = KernelExpr::struct_from([col!("b"), lit(1i64)]);
        assert_eq!(
            lower_typed(kernel, pq_output_schema().into()),
            "named_struct(Utf8(\"p\"), b, Utf8(\"q\"), Int64(1))"
        );
    }

    #[test]
    fn nested_struct_recurses_with_child_target_names() {
        let inner = KernelExpr::struct_from([col!("b"), lit(1i64)]);
        let kernel = KernelExpr::struct_from([inner]);
        let target =
            StructType::try_new([StructField::nullable("outer", pq_output_schema())]).unwrap();
        assert_eq!(
            lower_typed(kernel, target.into()),
            "named_struct(Utf8(\"outer\"), named_struct(Utf8(\"p\"), b, Utf8(\"q\"), Int64(1)))"
        );
    }

    #[test]
    fn struct_with_nullability_wraps_in_case() {
        let kernel = KernelExpr::struct_with_nullability_from(
            [col!("b"), lit(1i64)],
            KernelExpr::Predicate(Box::new(col!("x").is_not_null())),
        );
        // Kernel models IS NOT NULL as Not(IsNull), so the guard renders as "NOT x IS NULL".
        let rendered = lower_typed(kernel, pq_output_schema().into());
        assert!(
            rendered.starts_with("CASE WHEN NOT x IS NULL THEN named_struct("),
            "{rendered}"
        );
        assert!(rendered.ends_with("END"), "{rendered}");
    }

    #[test]
    fn struct_without_target_is_unsupported() {
        let kernel = KernelExpr::struct_from([col!("b")]);
        to_df_expr(&kernel, &test_schema(), None).unwrap_err();
    }

    #[test]
    fn struct_arity_mismatch_is_an_error() {
        let kernel = KernelExpr::struct_from([col!("b"), lit(1i64)]);
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
            lower_patch(patch, &ab_schema(), &pq_output_schema()),
            "named_struct(Utf8(\"p\"), a, Utf8(\"q\"), b)"
        );
    }

    #[test]
    fn top_level_patch_replace_puts_expr_in_field_slot() {
        let patch = ExpressionStructPatchBuilder::new()
            .replace("a", lit(7i64))
            .build()
            .unwrap();
        assert_eq!(
            lower_patch(patch, &ab_schema(), &pq_output_schema()),
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
            .prepend(lit(0i64))
            .append(lit(9i64))
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
            .insert_after("a", lit(5i64))
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
            .replace("a", lit(7i64))
            .build()
            .unwrap();
        assert_eq!(
            lower_patch(patch, &input, &pq_output_schema()),
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

    #[rstest]
    #[case::only_missing_required(
        ExpressionStructPatchBuilder::new()
            .replace("nonexistent", lit(1i64))
            .build()
            .unwrap(),
        pq_output_schema()
    )]
    #[case::matched_optional_does_not_mask_missing_required(
        ExpressionStructPatchBuilder::new()
            .drop_if_exists("b")
            .replace("nonexistent", lit(1i64))
            .build()
            .unwrap(),
        StructType::try_new([StructField::nullable("p", DataType::LONG)]).unwrap()
    )]
    fn required_patch_on_missing_field_is_an_error(
        #[case] patch: ExpressionStructPatch,
        #[case] target: StructType,
    ) {
        let expr = KernelExpr::struct_patch(patch).unwrap();
        let target: DataType = target.into();
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
            lower_patch(patch, &ab_schema(), &pq_output_schema()),
            "named_struct(Utf8(\"p\"), a, Utf8(\"q\"), b)"
        );
    }

    /// A struct target is re-derived and threaded at every nesting level: a `StructPatch` whose
    /// appended field `g` is a `Struct` whose field `h` is a `Struct` whose `leaf` is a column.
    /// Each level pulls its child's sub-schema from its own field type, so names land correctly all
    /// the way down (`g` from the patch target, `h` from g's sub-schema, `leaf` from h's).
    #[test]
    fn nested_struct_targets_are_rederived_at_each_level() {
        let deepest = KernelExpr::struct_from([col!("a")]); // { leaf: a }
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

    // === MapToStruct ===

    /// Input schema for map tests: `{ pv: map<string, string> }`.
    fn pv_map_schema() -> StructType {
        StructType::try_new([StructField::nullable(
            "pv",
            MapType::new(DataType::STRING, DataType::STRING, true),
        )])
        .unwrap()
    }

    /// Lowers a `MapToStruct` over `pv` targeting `output_schema` and renders it as a `Display`
    /// string.
    fn lower_map_to_struct(output_schema: StructType) -> String {
        let kernel = KernelExpr::map_to_struct(col!("pv"));
        let target: DataType = output_schema.into();
        to_df_expr(&kernel, &pv_map_schema(), Some(&target))
            .unwrap()
            .to_string()
    }

    /// Each target field extracts its value with `cast(get_field(pv, name), T)`, and the whole
    /// rebuild is wrapped in a null-map guard. Runtime cast/parse semantics (empty-string,
    /// temporal, decimal, duplicate keys, null masking) are arrow's, verified end-to-end rather
    /// than here.
    #[test]
    fn map_to_struct_lowers_to_named_struct_over_get_field() {
        let target = StructType::try_new([
            StructField::nullable("region", DataType::STRING),
            StructField::nullable("id", DataType::INTEGER),
        ])
        .unwrap();
        let rendered = lower_map_to_struct(target);
        assert_eq!(
            rendered,
            concat!(
                r#"CASE WHEN pv IS NOT NULL THEN named_struct("#,
                r#"Utf8("region"), CAST(get_field(pv, Utf8("region")) AS Utf8), "#,
                r#"Utf8("id"), CAST(nullif(get_field(pv, Utf8("id")), Utf8("")) AS Int32)) "#,
                r#"ELSE NULL END"#,
            )
        );
    }

    /// String and Binary targets keep the raw value (empty string is a valid value), so they lower
    /// to a bare `cast`; every other primitive first maps an empty string to null via `nullif`.
    #[rstest]
    #[case::string_bare_cast(DataType::STRING, "CAST(get_field(pv, Utf8(\"f\")) AS Utf8)")]
    #[case::binary_bare_cast(DataType::BINARY, "CAST(get_field(pv, Utf8(\"f\")) AS Binary)")]
    #[case::integer_wraps_nullif(
        DataType::INTEGER,
        "CAST(nullif(get_field(pv, Utf8(\"f\")), Utf8(\"\")) AS Int32)"
    )]
    fn map_to_struct_field_value_lowering(
        #[case] field_type: DataType,
        #[case] expected_value: &str,
    ) {
        let target = StructType::try_new([StructField::nullable("f", field_type)]).unwrap();
        let expected =
            format!("CASE WHEN pv IS NOT NULL THEN named_struct(Utf8(\"f\"), {expected_value}) ELSE NULL END");
        assert_eq!(lower_map_to_struct(target), expected);
    }

    /// The target must be a struct of primitive fields: an absent one leaves the rebuild without
    /// field names, and a non-primitive field has no string-to-value cast.
    #[rstest]
    #[case::no_target(None, "MapToStruct expression requires a struct output type")]
    #[case::non_primitive_field(
        Some(DataType::from(
            StructType::try_new([StructField::nullable("nested", pq_output_schema())]).unwrap()
        )),
        "MapToStruct only supports primitive target types, but field 'nested' is"
    )]
    fn map_to_struct_with_unsupported_target_is_an_error(
        #[case] output_type: Option<DataType>,
        #[case] expected_message: &str,
    ) {
        let kernel = KernelExpr::map_to_struct(col!("pv"));
        let err = to_df_expr(&kernel, &pv_map_schema(), output_type.as_ref())
            .unwrap_err()
            .to_string();
        assert!(err.contains(expected_message), "{err}");
    }

    // === ParseJson Shared Helpers ===

    /// Input schema for JSON tests: `{ j: string }`.
    fn json_input_schema() -> StructType {
        StructType::try_new([StructField::nullable("j", DataType::STRING)]).unwrap()
    }

    /// Target parse schema `{ n: long, s: string }`.
    fn parse_target() -> KernelSchemaRef {
        Arc::new(
            StructType::try_new([
                StructField::nullable("n", DataType::LONG),
                StructField::nullable("s", DataType::STRING),
            ])
            .unwrap(),
        )
    }

    /// Lowers `parse_json(col("j"), schema)`, builds a physical expr over a one-column string batch
    /// carrying `rows`, evaluates it, and returns the resulting struct column.
    fn eval_parse_json(schema: KernelSchemaRef, rows: Vec<Option<&str>>) -> StructArray {
        let kernel = KernelExpr::parse_json(column_expr!("j"), schema);
        // Self-typed: no output_type is threaded in, yet lowering still succeeds.
        let logical = to_df_expr(&kernel, &json_input_schema(), None).unwrap();

        let arrow_schema: ArrowSchema = (&json_input_schema()).try_into_arrow().unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(arrow_schema.clone()),
            vec![Arc::new(StringArray::from(rows)) as ArrayRef],
        )
        .unwrap();

        let df_schema = DFSchema::try_from(arrow_schema).unwrap();
        let physical = create_physical_expr(&logical, &df_schema, &ExecutionProps::new()).unwrap();
        physical
            .evaluate(&batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap()
            .as_struct()
            .clone()
    }

    // === ParseJson Tests ===

    #[test]
    fn parse_json_lowers_to_udf_call() {
        let kernel = KernelExpr::parse_json(column_expr!("j"), parse_target());
        assert_eq!(
            to_df_expr(&kernel, &json_input_schema(), None)
                .unwrap()
                .to_string(),
            "kernel_parse_json(j)"
        );
    }

    #[test]
    fn parse_json_parses_fields_into_typed_struct() {
        let out = eval_parse_json(
            parse_target(),
            vec![Some(r#"{"n": 1, "s": "a"}"#), Some(r#"{"n": 2, "s": "b"}"#)],
        );
        let n = out.column(0).as_primitive::<Int64Type>();
        let s = out.column(1).as_string::<i32>();
        assert_eq!(n.null_count(), 0);
        assert_eq!((n.value(0), n.value(1)), (1, 2));
        assert_eq!((s.value(0), s.value(1)), ("a", "b"));
    }

    /// A null input string decodes as `{}` (kernel's contract): the row stays present with all its
    /// fields null, rather than nulling the whole struct row.
    #[test]
    fn parse_json_null_input_yields_present_row_with_null_fields() {
        let out = eval_parse_json(parse_target(), vec![None]);
        assert_eq!(out.len(), 1);
        assert!(out.column(0).is_null(0) && out.column(1).is_null(0));
    }

    /// A field absent from the JSON object parses to null.
    #[test]
    fn parse_json_missing_field_is_null() {
        let out = eval_parse_json(parse_target(), vec![Some(r#"{"s": "only"}"#)]);
        assert!(out.column(0).is_null(0));
        assert_eq!(out.column(1).as_string::<i32>().value(0), "only");
    }

    /// Genuinely malformed JSON hits the coarse backstop: the whole struct comes back all-null
    /// (every field of every row null) rather than erroring the batch.
    #[test]
    fn parse_json_malformed_yields_all_null_struct() {
        let out = eval_parse_json(parse_target(), vec![Some("{not json"), Some(r#"{"n": 5}"#)]);
        assert_eq!(out.len(), 2);
        assert!((0..2).all(|i| out.column(0).is_null(i) && out.column(1).is_null(i)));
    }

    /// UDF identity must distinguish target schemas that share an arrow return type, or DataFusion
    /// would treat the two calls as one common subexpression and parse both with one schema.
    /// `integer` and `interval year to month` both map to arrow `Int32`.
    #[rstest]
    #[case(DataType::INTEGER, DataType::INTERVAL_YEAR_MONTH)]
    #[case(DataType::LONG, DataType::INTERVAL_DAY_TIME)]
    fn parse_json_udfs_with_same_return_type_but_different_schemas_are_not_equal(
        #[case] left: DataType,
        #[case] right: DataType,
    ) {
        let udf = |dt: DataType| {
            ParseJsonUdf::try_new(Arc::new(
                StructType::try_new([StructField::nullable("a", dt)]).unwrap(),
            ))
            .unwrap()
        };
        let (left, right) = (udf(left), udf(right));
        assert_eq!(
            left.return_type, right.return_type,
            "precondition: arrow return types collide"
        );
        assert_ne!(left, right);
    }
}
