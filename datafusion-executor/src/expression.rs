//! Conversion from a kernel [`Expression`](KernelExpression) to a DataFusion [`Expr`](DFExpr).

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{new_null_array, ArrayRef, RecordBatch, StructArray};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
use datafusion::common::utils::take_function_args;
use datafusion::common::{Column as DFColumn, DataFusionError, ScalarValue as DFScalarValue};
use datafusion::functions::core::expr_fn::{coalesce, get_field, named_struct, nullif};
use datafusion::functions_nested::expr_fn::make_array;
use datafusion::logical_expr::{
    binary_expr, cast, lit, Case, ColumnarValue, Expr as DFExpr, Operator, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::arrow_expression::evaluate_expression as kernel_expression;
use delta_kernel::engine::parse_json;
use delta_kernel::expressions::{
    BinaryExpression, BinaryExpressionOp, Expression as KernelExpression, ExpressionRef,
    ExpressionStructPatch, MapToStructExpression, MapToStructOptions, ParseJsonExpression,
    UnaryExpressionOp, VariadicExpression, VariadicExpressionOp,
};
use delta_kernel::schema::{
    DataType as KernelDataType, PrimitiveType, SchemaRef as KernelSchemaRef, StructField,
    StructType,
};
use delta_kernel::{DeltaResult, EngineData, Error};

use crate::predicate::to_df_predicate_expr;
use crate::scalar::to_df_scalar;
use crate::utils::column_to_df_expr;

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

/// Lowers [`KernelExpression::Struct`] or [`KernelExpression::StructPatch`] into named child
/// expressions and an optional row-level null guard. Callers can pack the children into a struct
/// or use them as individual columns.
///
/// # Errors
/// Returns an error if `expr` has another form, its field count disagrees with `output_type`, or a
/// field cannot be lowered.
pub(crate) fn to_df_struct_columns(
    expr: &KernelExpression,
    input_schema: &StructType,
    output_type: &StructType,
) -> DeltaResult<StructColumns> {
    match expr {
        KernelExpression::Struct(fields, nullability) => {
            struct_columns_from_fields(fields, nullability.as_ref(), input_schema, output_type)
        }
        KernelExpression::StructPatch(patch) => {
            struct_columns_from_patch(patch, input_schema, output_type)
        }
        _ => Err(Error::generic(format!(
            "Expression must be a Struct or StructPatch, got {expr:?}"
        ))),
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
pub(crate) fn struct_null_when_not(guard: DFExpr, body: DFExpr) -> DFExpr {
    DFExpr::Case(Case::new(
        None,
        vec![(Box::new(guard), Box::new(body))],
        Some(Box::new(lit(DFScalarValue::Null))),
    ))
}

/// A struct expression's named values and optional row-level null guard.
pub(crate) struct StructColumns {
    pairs: Vec<(String, DFExpr)>,
    null_guard: Option<DFExpr>,
}

impl StructColumns {
    /// Packs the columns into one `named_struct` value, preserving the struct's null mask.
    fn pack(self) -> DFExpr {
        // `named_struct` takes one flat arg list of alternating names and values:
        // `[name1, value1, name2, value2, ...]`, hence two args per field.
        let mut args = Vec::with_capacity(self.pairs.len() * 2);
        for (name, value) in self.pairs {
            args.push(lit(name));
            args.push(value);
        }
        let body = named_struct(args);
        match self.null_guard {
            Some(guard) => struct_null_when_not(guard, body),
            None => body,
        }
    }

    /// Returns flat output columns with the struct's null mask applied to each value.
    pub(crate) fn into_guarded_columns(self) -> Vec<(String, DFExpr)> {
        let Some(guard) = self.null_guard else {
            return self.pairs;
        };
        self.pairs
            .into_iter()
            .map(|(name, value)| (name, struct_null_when_not(guard.clone(), value)))
            .collect()
    }
}

/// Lowers a struct constructor to a `named_struct(..)` value, taking field names and per-child
/// target types from `output_type`. An optional nullability predicate nulls the whole struct where
/// it is not true.
fn struct_to_df_expr(
    fields: &[ExpressionRef],
    nullability: Option<&ExpressionRef>,
    input_schema: &StructType,
    output_type: Option<&KernelDataType>,
) -> DeltaResult<DFExpr> {
    let target = require_struct_output(output_type, "Struct")?;
    let columns = struct_columns_from_fields(fields, nullability, input_schema, target)?;
    Ok(columns.pack())
}

/// Builds the `(name, value)` columns of a struct constructor, taking field names and per-child
/// target types from `target`, plus the lowered nullability guard when present.
fn struct_columns_from_fields(
    fields: &[ExpressionRef],
    nullability: Option<&ExpressionRef>,
    input_schema: &StructType,
    target: &StructType,
) -> DeltaResult<StructColumns> {
    if fields.len() != target.num_fields() {
        return Err(Error::generic(format!(
            "Struct expression field count mismatch: {} fields in expression but {} in schema",
            fields.len(),
            target.num_fields()
        )));
    }
    let mut pairs = Vec::with_capacity(fields.len());
    for (child, field) in fields.iter().zip(target.fields()) {
        let value = to_df_expr(child, input_schema, Some(field.data_type()))?;
        pairs.push((field.name().to_string(), value));
    }
    let null_guard = nullability
        .map(|pred| to_df_expr(pred, input_schema, None))
        .transpose()?;
    Ok(StructColumns { pairs, null_guard })
}

/// Lowers a struct patch (a sparse edit of an input struct) to a `named_struct(..)` value. See
/// [`struct_columns_from_patch`] for the emission order and the nested-patch null semantics.
fn struct_patch_to_df_expr(
    patch: &ExpressionStructPatch,
    input_schema: &StructType,
    output_type: Option<&KernelDataType>,
) -> DeltaResult<DFExpr> {
    let target = require_struct_output(output_type, "StructPatch")?;
    let columns = struct_columns_from_patch(patch, input_schema, target)?;
    Ok(columns.pack())
}

/// Builds the `(name, value)` columns of a struct patch. Output field names come positionally from
/// `target`, whose corresponding field types are forwarded when lowering computed values. Walks the
/// evaluator's emission order: prepends, each input field (passed through unless dropped/replaced,
/// then its insertions), and appends. A nested patch (`input_path` set) reports a null guard on the
/// source struct row, matching the evaluator's preservation of the source struct's null buffer.
fn struct_columns_from_patch(
    patch: &ExpressionStructPatch,
    input_schema: &StructType,
    target: &StructType,
) -> DeltaResult<StructColumns> {
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
    // A nested patch must preserve its source struct's null bitmap. This predicate lets the caller
    // null every flattened output column wherever the source struct is null.
    let null_guard = source_expr.as_ref().map(|base| base.clone().is_not_null());

    // Append `(name, value)` pairs in the evaluator's emission order
    let mut output_fields = target.fields();
    let mut pairs: Vec<(String, DFExpr)> = Vec::with_capacity(target.num_fields());

    let append_converted = |pairs: &mut Vec<(String, DFExpr)>,
                            output_fields: &mut dyn Iterator<Item = &StructField>,
                            expr: &KernelExpression|
     -> DeltaResult<()> {
        let field = output_fields.next().ok_or_else(|| {
            Error::generic("StructPatch produced more fields than the output schema has")
        })?;
        let value = to_df_expr(expr, input_schema, Some(field.data_type()))?;
        pairs.push((field.name().to_string(), value));
        Ok(())
    };
    let append_existing = |pairs: &mut Vec<(String, DFExpr)>,
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
        pairs.push((field.name().to_string(), value));
        Ok(())
    };

    for expr in &patch.prepended_fields {
        append_converted(&mut pairs, &mut output_fields, expr)?;
    }

    // Should only count required field patches (excluding optional) for missing input fields
    // validation. An existing optional field can shadow a missing required field.
    let mut used_required_field_patches = 0usize;
    for input_field in source_struct.fields() {
        let name = input_field.name();
        let field_patch = patch.field_patches.get(name);

        if field_patch.is_none_or(|fp| fp.keep_input) {
            append_existing(&mut pairs, &mut output_fields, name)?;
        }

        let Some(field_patch) = field_patch else {
            continue;
        };
        for expr in &field_patch.insertions {
            append_converted(&mut pairs, &mut output_fields, expr)?;
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
        append_converted(&mut pairs, &mut output_fields, expr)?;
    }

    if output_fields.next().is_some() {
        return Err(Error::generic(
            "StructPatch produced fewer fields than the output schema has",
        ));
    }

    Ok(StructColumns { pairs, null_guard })
}

/// Lowers a `MapToStruct`, which reshapes a `Map<String, String>` into a struct by parsing each
/// value into its target field type. Field names and per-field types come from `output_type`,
/// which must be a struct containing only primitive fields.
///
/// Default options preserve the native DataFusion `named_struct(..)` lowering. Each field uses
/// `cast(get_field(map, name), T)`. Numeric and temporal values first pass through
/// `nullif(value, '')`, matching kernel's rule that an empty partition value becomes null, while
/// invalid non-empty values fail the cast. String and binary fields preserve empty values. Missing
/// keys and null values remain null, and a null input map produces a null struct.
///
/// The native lowering has known differences from the kernel parser for malformed or
/// non-spec-compliant input:
/// - Duplicate keys use the leftmost entry instead of the rightmost.
/// - Boolean casts also accept `yes`, `no`, `on`, `off`, `t`, `f`, `1`, and `0`.
/// - Decimal casts may rescale or round instead of requiring an exact target scale.
///
/// Configured options use a kernel-backed UDF so reader-timezone parsing follows kernel semantics.
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

    if map_to_struct.options == MapToStructOptions::default() {
        return lower_default_map_to_struct(map, target);
    }

    validate_map_to_struct_target(target)?;
    let udf =
        KernelMapToStructUdf::try_new(Arc::new(target.clone()), map_to_struct.options.clone())?;
    Ok(ScalarUDF::new_from_impl(udf).call(vec![map]))
}

fn lower_default_map_to_struct(map: DFExpr, target: &StructType) -> DeltaResult<DFExpr> {
    let mut args = Vec::with_capacity(target.num_fields() * 2);
    for field in target.fields() {
        let KernelDataType::Primitive(primitive) = field.data_type() else {
            return Err(Error::unsupported(format!(
                "MapToStruct only supports primitive target types, but field '{}' is {:?}",
                field.name(),
                field.data_type()
            )));
        };
        let raw = get_field(map.clone(), field.name().to_string());
        let value = match primitive {
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

fn validate_map_to_struct_target(target: &StructType) -> DeltaResult<()> {
    for field in target.fields() {
        if !matches!(field.data_type(), KernelDataType::Primitive(_)) {
            return Err(Error::unsupported(format!(
                "MapToStruct only supports primitive target types, but field '{}' is {:?}",
                field.name(),
                field.data_type()
            )));
        }
    }
    Ok(())
}

/// A DataFusion scalar UDF that delegates map-to-struct parsing to kernel's Arrow evaluator.
#[derive(Debug, PartialEq, Eq)]
struct KernelMapToStructUdf {
    output_schema: KernelSchemaRef,
    options: MapToStructOptions,
    return_type: ArrowDataType,
    signature: Signature,
}

impl std::hash::Hash for KernelMapToStructUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        hash_output_schema(self.output_schema.as_ref(), state);
        self.options.hash(state);
    }
}

impl KernelMapToStructUdf {
    fn try_new(output_schema: KernelSchemaRef, options: MapToStructOptions) -> DeltaResult<Self> {
        let arrow_schema: ArrowSchema = output_schema
            .as_ref()
            .try_into_arrow()
            .map_err(Error::generic_err)?;
        Ok(Self {
            return_type: ArrowDataType::Struct(arrow_schema.fields().clone()),
            output_schema,
            options,
            signature: Signature::any(1, Volatility::Immutable),
        })
    }
}

impl ScalarUDFImpl for KernelMapToStructUdf {
    fn name(&self) -> &str {
        "kernel_map_to_struct"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[ArrowDataType]) -> Result<ArrowDataType, DataFusionError> {
        Ok(self.return_type.clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue, DataFusionError> {
        let num_rows = args.number_rows;
        let [map] = take_function_args(self.name(), args.args)?;
        let batch = RecordBatch::try_from_iter([("map", map.into_array(num_rows)?)])?;
        let expression = KernelExpression::map_to_struct(
            KernelExpression::column(["map"]),
            self.options.clone(),
        );
        let output_type = KernelDataType::from(self.output_schema.as_ref().clone());
        let result =
            kernel_expression::evaluate_expression(&expression, &batch, Some(&output_type))
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(ColumnarValue::Array(result))
    }
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
    fn hash<H: Hasher>(&self, state: &mut H) {
        hash_output_schema(self.output_schema.as_ref(), state);
    }
}

fn hash_output_schema<H: Hasher>(schema: &StructType, state: &mut H) {
    for field in schema.fields() {
        field.name().hash(state);
        field.data_type().to_string().hash(state);
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
            // Coerces Utf8 / LargeUtf8 / Utf8View, mirroring kernel's `parse_json_impl`.
            signature: Signature::string(1, Volatility::Immutable),
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
    use datafusion::arrow::array::{
        Array, AsArray, Int32Array, MapBuilder, StringArray, StringBuilder,
        TimestampMicrosecondArray,
    };
    use datafusion::arrow::datatypes::Field as ArrowField;
    use datafusion::assert_batches_eq;
    use datafusion::common::DFSchema;
    use datafusion::physical_expr::create_physical_expr;
    use datafusion::physical_expr::execution_props::ExecutionProps;
    use delta_kernel::expressions::{
        col, lit, null_lit, ColumnName as KernelColumnName, Expression as KernelExpr,
        ExpressionStructPatch, ExpressionStructPatchBuilder, MapToStructOptions,
    };
    use delta_kernel::schema::{schema, schema_ref, ArrayType, DataType, MapType, StructType};
    use rstest::rstest;

    use super::*;

    /// Name-resolution scope for these tests: `a: { b: { c: long } }`, plus top-level `b` and `x`.
    fn test_schema() -> StructType {
        schema! {
            nullable "a": {
                nullable "b": {
                    nullable "c": LONG,
                },
            },
            nullable "b": LONG,
            nullable "x": LONG,
        }
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
    #[case::null(null_lit(DataType::LONG), "Int64(NULL)")]
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
        let leaf = schema! { nullable "p": LONG };
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
        schema! {
            nullable "p": LONG,
            nullable "q": LONG,
        }
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
        let target = schema! { nullable "outer": (pq_output_schema()) };
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
        let target: DataType = schema! { nullable "p": LONG }.into();
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
        schema! {
            nullable "a": LONG,
            nullable "b": LONG,
        }
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
        let target = schema! { nullable "q": LONG };
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
        let target = schema! {
            nullable "first": LONG,
            nullable "a": LONG,
            nullable "b": LONG,
            nullable "last": LONG,
        };
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
        let target = schema! {
            nullable "a": LONG,
            nullable "inserted": LONG,
            nullable "b": LONG,
        };
        assert_eq!(
            lower_patch(patch, &ab_schema(), &target),
            "named_struct(Utf8(\"a\"), a, Utf8(\"inserted\"), Int64(5), Utf8(\"b\"), b)"
        );
    }

    #[test]
    fn nested_patch_wraps_in_null_guard_case() {
        // Input schema: { s: { a: long, b: long } }. Patch replaces s.a with a literal.
        let input = schema! { nullable "s": (ab_schema()) };
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
        let target: DataType = schema! {
            nullable "p": LONG,
            nullable "q": LONG,
            nullable "r": LONG,
        }
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
        let target: DataType = schema! { nullable "p": LONG }.into();
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
        schema! { nullable "p": LONG }
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
        let target = schema! {
            nullable "a": LONG,
            nullable "b": LONG,
            nullable "g": {
                nullable "h": {
                    nullable "leaf": LONG,
                },
            },
        };
        assert_eq!(
            lower_patch(patch, &ab_schema(), &target),
            "named_struct(Utf8(\"a\"), a, Utf8(\"b\"), b, Utf8(\"g\"), \
             named_struct(Utf8(\"h\"), named_struct(Utf8(\"leaf\"), a)))"
        );
    }

    // === MapToStruct ===

    /// Input schema for map tests: `{ pv: map<string, string> }`.
    fn pv_map_schema() -> StructType {
        schema! { nullable "pv": { STRING => nullable STRING } }
    }

    fn lower_map_to_struct(output_schema: StructType, options: MapToStructOptions) -> String {
        let kernel = KernelExpr::map_to_struct(col!("pv"), options);
        let target: DataType = output_schema.into();
        to_df_expr(&kernel, &pv_map_schema(), Some(&target))
            .unwrap()
            .to_string()
    }

    #[test]
    fn default_map_to_struct_retains_native_lowering() {
        let target = schema! {
            nullable "region": STRING,
            nullable "id": INTEGER,
        };
        assert_eq!(
            lower_map_to_struct(target, MapToStructOptions::default()),
            concat!(
                r#"CASE WHEN pv IS NOT NULL THEN named_struct("#,
                r#"Utf8("region"), CAST(get_field(pv, Utf8("region")) AS Utf8), "#,
                r#"Utf8("id"), CAST(nullif(get_field(pv, Utf8("id")), Utf8("")) AS Int32)) "#,
                r#"ELSE NULL END"#,
            )
        );
    }

    #[test]
    fn configured_map_to_struct_lowers_to_kernel_udf() {
        let target = schema! {
            nullable "region": STRING,
            nullable "id": INTEGER,
        };
        let options = MapToStructOptions::default().with_timestamp_timezone("America/Los_Angeles");
        assert_eq!(
            lower_map_to_struct(target, options),
            "kernel_map_to_struct(pv)"
        );
    }

    /// The target must be a struct of primitive fields: an absent one provides no field names,
    /// and a non-primitive field is outside map-to-struct's parsing contract.
    #[rstest]
    #[case::no_target(None, "MapToStruct expression requires a struct output type")]
    #[case::non_primitive_field(
        Some(DataType::from(schema! {
            nullable "nested": (pq_output_schema()),
        })),
        "MapToStruct only supports primitive target types, but field 'nested' is"
    )]
    fn map_to_struct_with_unsupported_target_is_an_error(
        #[values(
            MapToStructOptions::default(),
            MapToStructOptions::default().with_timestamp_timezone("America/Los_Angeles")
        )]
        options: MapToStructOptions,
        #[case] output_type: Option<DataType>,
        #[case] expected_message: &str,
    ) {
        let kernel = KernelExpr::map_to_struct(col!("pv"), options);
        let err = to_df_expr(&kernel, &pv_map_schema(), output_type.as_ref())
            .unwrap_err()
            .to_string();
        assert!(err.contains(expected_message), "{err}");
    }

    #[rstest]
    #[case::default_utc(MapToStructOptions::default(), 1_718_443_800_000_000)]
    #[case::reader_timezone(
        MapToStructOptions::default().with_timestamp_timezone("America/Los_Angeles"),
        1_718_469_000_000_000
    )]
    fn map_to_struct_executes_with_options(
        #[case] options: MapToStructOptions,
        #[case] expected_timestamp: i64,
    ) {
        let mut maps = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        maps.keys().append_value("id");
        maps.values().append_value("7");
        maps.keys().append_value("ts");
        maps.values().append_value("2024-06-15 09:30:00");
        maps.append(true).unwrap();
        maps.append(false).unwrap();
        let map = Arc::new(maps.finish()) as ArrayRef;
        let arrow_schema =
            ArrowSchema::new(vec![ArrowField::new("pv", map.data_type().clone(), true)]);
        let batch = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![map]).unwrap();
        let target = schema! {
            nullable "id": INTEGER,
            nullable "ts": TIMESTAMP,
        };
        let logical = to_df_expr(
            &KernelExpr::map_to_struct(col!("pv"), options),
            &pv_map_schema(),
            Some(&DataType::from(target)),
        )
        .unwrap();
        let df_schema = DFSchema::try_from(arrow_schema).unwrap();
        let physical = create_physical_expr(&logical, &df_schema, &ExecutionProps::new()).unwrap();
        let result = physical.evaluate(&batch).unwrap().into_array(2).unwrap();
        let result = result.as_struct();
        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let timestamps = result
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ids.value(0), 7);
        assert_eq!(timestamps.value(0), expected_timestamp);
        assert!(result.is_null(1));
    }

    // === ParseJson Shared Helpers ===

    /// Input schema for JSON tests: `{ j: string }`.
    fn json_input_schema() -> StructType {
        schema! { nullable "j": STRING }
    }

    fn nested_parse_type() -> StructType {
        schema! {
            nullable "n": LONG,
            nullable "s": STRING,
        }
    }

    /// Target parse schema `{ n: long, s: string }`.
    fn parse_target() -> KernelSchemaRef {
        Arc::new(nested_parse_type())
    }

    /// Lowers `parse_json(col("j"), schema)`, builds a physical expr over a one-column string batch
    /// carrying `rows`, evaluates it, and returns the resulting struct column.
    fn eval_parse_json(schema: KernelSchemaRef, rows: Vec<Option<&str>>) -> StructArray {
        let kernel = KernelExpr::parse_json(col!("j"), schema);
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

    /// [`eval_parse_json`] with the struct flattened to one column per parsed field. Panics on a
    /// struct with a top-level null, so the malformed-backstop case must use [`eval_parse_json`].
    fn eval_parse_json_batch(schema: KernelSchemaRef, rows: Vec<Option<&str>>) -> RecordBatch {
        RecordBatch::from(eval_parse_json(schema, rows))
    }

    /// Asserts the result fields equal `target`'s arrow projection: the parse is typed to the
    /// target schema, not merely compatible with it.
    fn assert_matches_target(batch: &RecordBatch, target: &KernelSchemaRef) {
        let target: ArrowSchema = target.as_ref().try_into_arrow().unwrap();
        assert_eq!(batch.schema().fields(), target.fields());
    }

    // === ParseJson Tests ===

    #[test]
    fn parse_json_lowers_to_udf_call() {
        let kernel = KernelExpr::parse_json(col!("j"), parse_target());
        assert_eq!(
            to_df_expr(&kernel, &json_input_schema(), None)
                .unwrap()
                .to_string(),
            "kernel_parse_json(j)"
        );
    }

    #[test]
    fn parse_json_parses_fields_into_typed_struct() {
        let batch = eval_parse_json_batch(
            parse_target(),
            vec![Some(r#"{"n": 1, "s": "a"}"#), Some(r#"{"n": 2, "s": "b"}"#)],
        );
        assert_matches_target(&batch, &parse_target());
        assert_batches_eq!(
            [
                "+---+---+",
                "| n | s |",
                "+---+---+",
                "| 1 | a |",
                "| 2 | b |",
                "+---+---+",
            ],
            &[batch]
        );
    }

    /// Every primitive `parse_json` can decode, in one struct: the integer/float/boolean families
    /// decode directly, while the failure-prone leaves (date, both timestamps, decimal) route
    /// through kernel's stringify-then-safe-cast path. Asserts they all land typed to the target.
    #[test]
    fn parse_json_decodes_all_supported_primitive_types() {
        let target: KernelSchemaRef = schema_ref! {
            nullable "str": STRING,
            nullable "long": LONG,
            nullable "int": INTEGER,
            nullable "short": SHORT,
            nullable "byte": BYTE,
            nullable "float": FLOAT,
            nullable "double": DOUBLE,
            nullable "bool": BOOLEAN,
            nullable "date": DATE,
            nullable "ts": TIMESTAMP,
            nullable "ts_ntz": TIMESTAMP_NTZ,
            nullable "dec": (DataType::decimal(10, 2).unwrap()),
        };
        let row = r#"{
            "str": "a", "long": 1, "int": 2, "short": 3, "byte": 4,
            "float": 1.5, "double": 2.5, "bool": true, "date": "2024-01-02",
            "ts": "2024-01-02T03:04:05Z", "ts_ntz": "2024-01-02T03:04:05", "dec": "12.34"
        }"#;
        let batch = eval_parse_json_batch(target.clone(), vec![Some(row)]);
        assert_matches_target(&batch, &target);
        assert_batches_eq!(
            [
                "+-----+------+-----+-------+------+-------+--------+------+------------+----------------------+---------------------+-------+",
                "| str | long | int | short | byte | float | double | bool | date       | ts                   | ts_ntz              | dec   |",
                "+-----+------+-----+-------+------+-------+--------+------+------------+----------------------+---------------------+-------+",
                "| a   | 1    | 2   | 3     | 4    | 1.5   | 2.5    | true | 2024-01-02 | 2024-01-02T03:04:05Z | 2024-01-02T03:04:05 | 12.34 |",
                "+-----+------+-----+-------+------+-------+--------+------+------------+----------------------+---------------------+-------+",
            ],
            &[batch]
        );
    }

    #[rstest]
    #[case::array(
        DataType::from(ArrayType::new(DataType::INTEGER, true)),
        r#"[1, null, 3]"#,
        "[1, , 3]"
    )]
    #[case::struct_(
        DataType::from(nested_parse_type()),
        r#"{"n": 1, "s": "a"}"#,
        "{n: 1, s: a}"
    )]
    #[case::map(
        DataType::from(MapType::new(DataType::STRING, DataType::LONG, true)),
        r#"{"x": 1, "y": null}"#,
        "{x: 1, y: }"
    )]
    #[case::array_of_structs(
        DataType::from(ArrayType::new(nested_parse_type(), true)),
        r#"[{"n": 1, "s": "a"}, null, {"n": 2, "s": "b"}]"#,
        "[{n: 1, s: a}, , {n: 2, s: b}]"
    )]
    #[case::array_of_maps(
        DataType::from(ArrayType::new(
            MapType::new(DataType::STRING, DataType::LONG, true),
            true,
        )),
        r#"[{"x": 1, "y": null}, {"z": 2}]"#,
        "[{x: 1, y: }, {z: 2}]"
    )]
    #[case::array_of_arrays(
        DataType::from(ArrayType::new(ArrayType::new(DataType::INTEGER, true), true)),
        r#"[[1, null], [2, 3]]"#,
        "[[1, ], [2, 3]]"
    )]
    #[case::struct_of_structs(
        DataType::from(schema! { nullable "inner": (nested_parse_type()) }),
        r#"{"inner": {"n": 1, "s": "a"}}"#,
        "{inner: {n: 1, s: a}}"
    )]
    #[case::struct_of_arrays(
        DataType::from(schema! { nullable "items": [ nullable INTEGER ] }),
        r#"{"items": [1, null, 3]}"#,
        "{items: [1, , 3]}"
    )]
    #[case::struct_of_maps(
        DataType::from(schema! { nullable "items": { STRING => nullable LONG } }),
        r#"{"items": {"x": 1, "y": null}}"#,
        "{items: {x: 1, y: }}"
    )]
    #[case::map_of_structs(
        DataType::from(MapType::new(DataType::STRING, nested_parse_type(), true)),
        r#"{"x": {"n": 1, "s": "a"}, "y": {"n": 2, "s": "b"}}"#,
        "{x: {n: 1, s: a}, y: {n: 2, s: b}}"
    )]
    #[case::map_of_arrays(
        DataType::from(MapType::new(
            DataType::STRING,
            ArrayType::new(DataType::INTEGER, true),
            true,
        )),
        r#"{"x": [1, null], "y": [2, 3]}"#,
        "{x: [1, ], y: [2, 3]}"
    )]
    #[case::map_of_maps(
        DataType::from(MapType::new(
            DataType::STRING,
            MapType::new(DataType::STRING, DataType::LONG, true),
            true,
        )),
        r#"{"x": {"a": 1}, "y": {"b": 2}}"#,
        "{x: {a: 1}, y: {b: 2}}"
    )]
    fn parse_json_decodes_nested_container_field(
        #[case] field_type: DataType,
        #[case] json_value: &str,
        #[case] expected_value: &str,
    ) {
        let target: KernelSchemaRef = schema_ref! { nullable "value": (field_type) };
        let row = format!(r#"{{"value": {json_value}}}"#);
        let batch = eval_parse_json_batch(target.clone(), vec![Some(row.as_str())]);
        assert_matches_target(&batch, &target);

        let width = expected_value.len().max("value".len());
        let border = format!("+{}+", "-".repeat(width + 2));
        let header = format!("| {:width$} |", "value");
        let value = format!("| {expected_value:width$} |");
        let expected = [
            border.as_str(),
            header.as_str(),
            border.as_str(),
            value.as_str(),
            border.as_str(),
        ];
        assert_batches_eq!(expected, &[batch]);
    }

    /// `Binary` has no JSON decoder in arrow-json, so any row hits kernel's whole-batch parse error
    /// and the coarse backstop nulls the struct. Documents that a `Binary` leaf is effectively
    /// unsupported through this path rather than silently mis-decoding.
    #[test]
    fn parse_json_binary_leaf_is_unsupported_and_yields_all_null_struct() {
        let target: KernelSchemaRef = schema_ref! { nullable "b": BINARY };
        let out = eval_parse_json(target, vec![Some(r#"{"b": "aGk="}"#)]);
        assert_eq!(out.len(), 1);
        assert!(out.column(0).is_null(0));
    }

    /// A null input string decodes as `{}` (kernel's contract): the row stays present with all its
    /// fields null, rather than nulling the whole struct row.
    #[test]
    fn parse_json_null_input_yields_present_row_with_null_fields() {
        let batch = eval_parse_json_batch(parse_target(), vec![None]);
        assert_batches_eq!(
            [
                "+---+---+",
                "| n | s |",
                "+---+---+",
                "|   |   |",
                "+---+---+",
            ],
            &[batch]
        );
    }

    /// A field absent from the JSON object parses to null.
    #[test]
    fn parse_json_missing_field_is_null() {
        let batch = eval_parse_json_batch(parse_target(), vec![Some(r#"{"s": "only"}"#)]);
        assert_batches_eq!(
            [
                "+---+------+",
                "| n | s    |",
                "+---+------+",
                "|   | only |",
                "+---+------+",
            ],
            &[batch]
        );
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
        let udf = |dt: DataType| ParseJsonUdf::try_new(schema_ref! { nullable "a": (dt) }).unwrap();
        let (left, right) = (udf(left), udf(right));
        assert_eq!(
            left.return_type, right.return_type,
            "precondition: arrow return types collide"
        );
        assert_ne!(left, right);
    }
}
