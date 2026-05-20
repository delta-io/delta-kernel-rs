//! Kernel [`Expression`] / [`Predicate`] -> DataFusion [`Expr`] translation.
//!
//! Single entry point: [`kernel_expr_to_df`] takes a kernel expression and a
//! [`TranslationContext`] (optional output-field target + optional input schema).
//!
//! Cast policy: primitive targets are wrapped in `cast(..)`; nested-container targets
//! (Struct/Array/Map) trust the producing arm to emit the right shape, because DataFusion's
//! `cast` on these is name-based and silently breaks column-mapping reshape.

use std::sync::Arc;

use datafusion_common::arrow::array::StructArray;
use datafusion_common::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Fields as ArrowFields, Schema as ArrowSchema,
};
use datafusion_common::error::DataFusionError;
use datafusion_common::{Column, ScalarValue};
use datafusion_expr::expr::{BinaryExpr, Case, InList, LambdaVariable};
use datafusion_expr::expr_fn::{cast, lambda};
use datafusion_expr::{lit, Expr, Operator};
use datafusion_functions::core::expr_fn::{get_field, named_struct, r#struct as make_struct};
use datafusion_functions_nested::expr_fn::{array_transform, make_array};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::expressions::{
    BinaryExpression, BinaryExpressionOp, BinaryPredicate, BinaryPredicateOp, ColumnName,
    Expression, IfExpression, JunctionPredicate, JunctionPredicateOp, MapToStructExpression,
    ParseJsonExpression, Predicate, Scalar, StructData, Transform, UnaryPredicate,
    UnaryPredicateOp, VariadicExpression, VariadicExpressionOp,
};
use delta_kernel::schema::{DataType, PrimitiveType, StructField, StructType};

use crate::compile::json_parse;
use crate::error::unsupported;

/// Optional target context for [`kernel_expr_to_df`]. See module docs for the contract.
#[derive(Default, Clone, Copy)]
pub struct TranslationContext<'a> {
    pub output_field: Option<&'a StructField>,
    /// Required by identity-`Transform` to resolve source struct physical field names.
    pub input_schema: Option<&'a ArrowSchema>,
}

impl<'a> TranslationContext<'a> {
    pub fn untyped() -> Self {
        Self::default()
    }

    pub fn typed(output_field: &'a StructField, input_schema: &'a ArrowSchema) -> Self {
        Self {
            output_field: Some(output_field),
            input_schema: Some(input_schema),
        }
    }
}

/// Translate a slice of kernel `Arc<Expression>`s with the same context.
pub fn kernel_exprs_to_df(
    exprs: &[Arc<Expression>],
    cx: &TranslationContext<'_>,
) -> Result<Vec<Expr>, DataFusionError> {
    exprs.iter().map(|e| kernel_expr_to_df(e, cx)).collect()
}

/// Shorthand for translating a kernel `Expression` without a target context. The vast majority
/// of internal call sites do not have an output field on hand (only the outer projection arm
/// in `compile_project_node` does); collapsing the `&TranslationContext::untyped()` boilerplate
/// into one name keeps the natural-type arms readable.
pub fn kernel_expr_to_df_untyped(expr: &Expression) -> Result<Expr, DataFusionError> {
    kernel_expr_to_df(expr, &TranslationContext::untyped())
}

/// Slice form of [`kernel_expr_to_df_untyped`].
pub fn kernel_exprs_to_df_untyped(exprs: &[Arc<Expression>]) -> Result<Vec<Expr>, DataFusionError> {
    kernel_exprs_to_df(exprs, &TranslationContext::untyped())
}

/// Translate a kernel [`Expression`] to a DataFusion [`Expr`].
pub fn kernel_expr_to_df(
    expr: &Expression,
    cx: &TranslationContext<'_>,
) -> Result<Expr, DataFusionError> {
    match expr {
        Expression::ParseJson(parse_json) => return parse_json_to_df(parse_json),
        Expression::MapToStruct(map_to_struct) => return map_to_struct_to_df(map_to_struct, cx),
        Expression::Transform(transform) => return transform_to_df(transform, cx),
        Expression::Struct(children, nullability_predicate) => {
            return struct_to_df(children.as_slice(), nullability_predicate.as_ref(), cx);
        }
        _ => {}
    }
    let raw = match expr {
        Expression::Literal(scalar) => Expr::Literal(scalar_value_to_df(scalar)?, None),
        Expression::Column(name) => column_to_df(name)?,
        Expression::Predicate(pred) => kernel_pred_to_df(pred)?,
        Expression::Binary(BinaryExpression { op, left, right }) => {
            binary_expr_to_df(*op, left, right)?
        }
        Expression::Variadic(VariadicExpression { op, exprs }) => variadic_to_df(*op, exprs, cx)?,
        Expression::If(if_expr) => if_to_df(if_expr, cx)?,
        Expression::Unary(_) => {
            return Err(unsupported(
                "expr_translator: Unary(ToJson) is not yet supported",
            ))
        }
        Expression::Opaque(_) => {
            return Err(unsupported(
                "expr_translator: Opaque expressions are not supported (engine-defined ops \
                 do not round-trip through DataFusion logical plans)",
            ))
        }
        Expression::Unknown(name) => {
            return Err(unsupported(format!(
                "expr_translator: Unknown expression {name:?} cannot be translated"
            )))
        }
        // Already handled above and returned early.
        Expression::ParseJson(_)
        | Expression::MapToStruct(_)
        | Expression::Transform(_)
        | Expression::Struct(_, _) => {
            return Err(crate::error::internal_error(
                "expr_translator: target-shaping arm fell through; should have returned early",
            ));
        }
    };
    apply_target_cast(raw, cx)
}

/// Translate a kernel [`Predicate`] to a boolean-typed DataFusion [`Expr`].
pub fn kernel_pred_to_df(pred: &Predicate) -> Result<Expr, DataFusionError> {
    match pred {
        Predicate::BooleanExpression(expr) => kernel_expr_to_df_untyped(expr),
        Predicate::Not(inner) => Ok(Expr::Not(Box::new(kernel_pred_to_df(inner)?))),
        Predicate::Unary(UnaryPredicate { op, expr }) => unary_pred_to_df(*op, expr),
        Predicate::Binary(BinaryPredicate { op, left, right }) => {
            binary_pred_to_df(*op, left, right)
        }
        Predicate::Junction(JunctionPredicate { op, preds }) => junction_to_df(*op, preds),
        Predicate::Opaque(_) => Err(unsupported(
            "expr_translator: Opaque predicates are not supported",
        )),
        Predicate::Unknown(name) => Err(unsupported(format!(
            "expr_translator: Unknown predicate {name:?} cannot be translated"
        ))),
    }
}

// === Target-shaping arms (each emits cx.output_field's declared shape) ===

/// `ParseJson` carries its own `output_schema`; works in any context (typed or untyped).
fn parse_json_to_df(parse_json: &ParseJsonExpression) -> Result<Expr, DataFusionError> {
    let json_expr = kernel_expr_to_df_untyped(parse_json.json_expr.as_ref())?;
    let extracted = json_parse::generate_schema_extractions(&json_expr, &parse_json.output_schema)?;
    let mut args = Vec::with_capacity(extracted.len() * 2);
    for (field_expr, field_name) in extracted {
        args.push(lit(field_name));
        args.push(field_expr);
    }
    Ok(named_struct(args))
}

/// `MapToStruct` requires a Struct target in `cx.output_field` (target field list drives
/// extraction). Errors if the context is untyped or the target isn't a Struct.
fn map_to_struct_to_df(
    map_to_struct: &MapToStructExpression,
    cx: &TranslationContext<'_>,
) -> Result<Expr, DataFusionError> {
    let target_field = cx.output_field.ok_or_else(|| {
        unsupported(
            "MapToStruct requires a typed projection context (Struct target); reached \
             without one (the enclosing Project/Coalesce/If didn't propagate target type).",
        )
    })?;
    let DataType::Struct(target_struct) = target_field.data_type() else {
        return Err(crate::error::plan_compilation(format!(
            "MapToStruct projection requires Struct output type, got {:?}",
            target_field.data_type()
        )));
    };
    let map_expr = kernel_expr_to_df_untyped(map_to_struct.map_expr.as_ref())?;
    let mut args = Vec::with_capacity(target_struct.fields().count() * 2);
    for child in target_struct.fields() {
        let arrow_ty: ArrowDataType = child.data_type().try_into_arrow().map_err(|e| {
            crate::error::plan_compilation(format!(
                "MapToStruct target field `{}` type conversion failed: {e}",
                child.name()
            ))
        })?;
        let raw_value = get_field(map_expr.clone(), child.name().to_string());
        args.push(lit(child.name().to_string()));
        args.push(cast(raw_value, arrow_ty));
    }
    Ok(named_struct(args))
}

/// Identity `Transform` with a Struct target encodes column-mapping physical->logical
/// rename for struct columns. Non-identity transforms are not yet supported.
fn transform_to_df(
    transform: &Transform,
    cx: &TranslationContext<'_>,
) -> Result<Expr, DataFusionError> {
    if !transform.is_identity() {
        return Err(unsupported(
            "Non-identity Transform expressions are not yet supported",
        ));
    }
    let target_field = cx
        .output_field
        .ok_or_else(|| unsupported("identity Transform requires a typed projection context"))?;
    let target_struct = match target_field.data_type() {
        DataType::Struct(s) => s.as_ref(),
        other => {
            return Err(crate::error::plan_compilation(format!(
                "Identity Transform projection requires Struct output type, got {other:?}"
            )))
        }
    };
    let input_path = transform.input_path().ok_or_else(|| {
        unsupported("Top-level identity Transform without input_path is not supported")
    })?;
    let input_schema = cx
        .input_schema
        .ok_or_else(|| unsupported("identity Transform requires an input schema in context"))?;
    let source_fields = lookup_struct_fields_via_path(input_schema, input_path)?;
    let input_expr = kernel_expr_to_df_untyped(&Expression::Column(input_path.clone()))?;
    rebuild_struct_with_target_names(input_expr, &source_fields, target_struct)
}

/// Typed Struct target with matching arity: `named_struct(...)` using target field names
/// (column-mapping renames flow through). Otherwise positional `make_struct(...)`. An
/// optional nullability predicate wraps the result in `CASE WHEN p THEN s ELSE NULL END`.
fn struct_to_df(
    children: &[Arc<Expression>],
    nullability_predicate: Option<&Arc<Expression>>,
    cx: &TranslationContext<'_>,
) -> Result<Expr, DataFusionError> {
    let body = match cx.output_field.map(|f| f.data_type()) {
        Some(DataType::Struct(target_struct))
            if target_struct.fields().count() == children.len() =>
        {
            let mut args = Vec::with_capacity(children.len() * 2);
            for (child_expr, child_field) in children.iter().zip(target_struct.fields()) {
                let child_cx = TranslationContext {
                    output_field: Some(child_field),
                    input_schema: cx.input_schema,
                };
                args.push(lit(child_field.name().to_string()));
                args.push(kernel_expr_to_df(child_expr.as_ref(), &child_cx)?);
            }
            named_struct(args)
        }
        _ => {
            let positional = children
                .iter()
                .map(|e| kernel_expr_to_df_untyped(e.as_ref()))
                .collect::<Result<Vec<_>, _>>()?;
            make_struct(positional)
        }
    };
    match nullability_predicate {
        None => Ok(body),
        // Kernel semantics: predicate false/null -> struct null. Same as DataFusion CASE.
        Some(predicate) => {
            let pred_expr = kernel_expr_to_df_untyped(predicate.as_ref())?;
            Ok(Expr::Case(Case::new(
                None,
                vec![(Box::new(pred_expr), Box::new(body))],
                Some(Box::new(Expr::Literal(ScalarValue::Null, None))),
            )))
        }
    }
}

// === Natural-type arms + cx-propagating compound arms ===

fn column_to_df(name: &ColumnName) -> Result<Expr, DataFusionError> {
    let mut parts = name.iter();
    let root = parts
        .next()
        .ok_or_else(|| unsupported("expr_translator: empty column path cannot be translated"))?;
    Ok(parts.fold(
        Expr::Column(Column::new_unqualified(root)),
        |expr, field| get_field(expr, field.as_str()),
    ))
}

fn binary_expr_to_df(
    op: BinaryExpressionOp,
    left: &Expression,
    right: &Expression,
) -> Result<Expr, DataFusionError> {
    let df_op = match op {
        BinaryExpressionOp::Plus => Operator::Plus,
        BinaryExpressionOp::Minus => Operator::Minus,
        BinaryExpressionOp::Multiply => Operator::Multiply,
        BinaryExpressionOp::Divide => Operator::Divide,
    };
    Ok(binary(
        kernel_expr_to_df_untyped(left)?,
        df_op,
        kernel_expr_to_df_untyped(right)?,
    ))
}

fn unary_pred_to_df(op: UnaryPredicateOp, expr: &Expression) -> Result<Expr, DataFusionError> {
    let inner = kernel_expr_to_df_untyped(expr)?;
    match op {
        UnaryPredicateOp::IsNull => Ok(Expr::IsNull(Box::new(inner))),
    }
}

fn binary_pred_to_df(
    op: BinaryPredicateOp,
    left: &Expression,
    right: &Expression,
) -> Result<Expr, DataFusionError> {
    // `In` is special: kernel models it as `Binary(In, value, array_literal)` where the right
    // side is a constant `Scalar::Array`. DataFusion's `Expr::InList` carries the list as a
    // Vec<Expr>.
    let df_op = match op {
        BinaryPredicateOp::In => return in_pred_to_df(left, right),
        BinaryPredicateOp::Equal => Operator::Eq,
        BinaryPredicateOp::LessThan => Operator::Lt,
        BinaryPredicateOp::GreaterThan => Operator::Gt,
        BinaryPredicateOp::Distinct => Operator::IsDistinctFrom,
    };
    Ok(binary(
        kernel_expr_to_df_untyped(left)?,
        df_op,
        kernel_expr_to_df_untyped(right)?,
    ))
}

fn in_pred_to_df(value: &Expression, list: &Expression) -> Result<Expr, DataFusionError> {
    let Expression::Literal(Scalar::Array(arr)) = list else {
        return Err(unsupported(format!(
            "expr_translator: IN predicate requires a literal array on the right; got {list:?}"
        )));
    };
    let elements = arr
        .array_elements()
        .iter()
        .map(|s| scalar_value_to_df(s).map(|v| Expr::Literal(v, None)))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Expr::InList(InList::new(
        Box::new(kernel_expr_to_df_untyped(value)?),
        elements,
        false,
    )))
}

fn junction_to_df(op: JunctionPredicateOp, preds: &[Predicate]) -> Result<Expr, DataFusionError> {
    let df_op = match op {
        JunctionPredicateOp::And => Operator::And,
        JunctionPredicateOp::Or => Operator::Or,
    };
    let mut iter = preds.iter().map(kernel_pred_to_df);
    let first = iter.next().ok_or_else(|| {
        unsupported("expr_translator: empty Junction (And/Or) cannot be lowered")
    })??;
    iter.try_fold(first, |acc, next| Ok(binary(acc, df_op, next?)))
}

/// `Coalesce` propagates `cx` to every branch (all branches must produce the target type).
/// `Array` translates elements untyped (kernel doesn't carry element type in `output_field`).
fn variadic_to_df(
    op: VariadicExpressionOp,
    exprs: &[Expression],
    cx: &TranslationContext<'_>,
) -> Result<Expr, DataFusionError> {
    match op {
        VariadicExpressionOp::Coalesce if exprs.is_empty() => Err(unsupported(
            "expr_translator: COALESCE() requires at least one argument",
        )),
        VariadicExpressionOp::Coalesce => {
            let args: Vec<Expr> = exprs
                .iter()
                .map(|e| kernel_expr_to_df(e, cx))
                .collect::<Result<_, _>>()?;
            Ok(datafusion_functions::core::expr_fn::coalesce(args))
        }
        VariadicExpressionOp::Array => {
            let args: Vec<Expr> = exprs
                .iter()
                .map(kernel_expr_to_df_untyped)
                .collect::<Result<_, _>>()?;
            Ok(make_array(args))
        }
    }
}

/// `If(condition, then, else)` -> `Expr::Case`. Then/else inherit `cx` so the same target
/// type applies to both arms.
fn if_to_df(if_expr: &IfExpression, cx: &TranslationContext<'_>) -> Result<Expr, DataFusionError> {
    let cond = Box::new(kernel_pred_to_df(&if_expr.condition)?);
    let then = Box::new(kernel_expr_to_df(&if_expr.then_expr, cx)?);
    let r#else = Box::new(kernel_expr_to_df(&if_expr.else_expr, cx)?);
    Ok(Expr::Case(Case::new(
        None,
        vec![(cond, then)],
        Some(r#else),
    )))
}

/// Wrap `raw` in `cast(..)` for primitive targets; pass through for nested targets.
fn apply_target_cast(raw: Expr, cx: &TranslationContext<'_>) -> Result<Expr, DataFusionError> {
    let Some(field) = cx.output_field else {
        return Ok(raw);
    };
    if matches!(
        field.data_type(),
        DataType::Struct(_) | DataType::Array(_) | DataType::Map(_)
    ) {
        return Ok(raw);
    }
    let arrow_ty: ArrowDataType = field.data_type().try_into_arrow().map_err(|e| {
        crate::error::plan_compilation(format!(
            "kernel_expr_to_df: target type conversion failed for `{}`: {e}",
            field.name()
        ))
    })?;
    Ok(cast(raw, arrow_ty))
}

// === Scalar / NULL conversion helpers ===

fn scalar_value_to_df(scalar: &Scalar) -> Result<ScalarValue, DataFusionError> {
    Ok(match scalar {
        Scalar::Integer(v) => ScalarValue::Int32(Some(*v)),
        Scalar::Long(v) => ScalarValue::Int64(Some(*v)),
        Scalar::Short(v) => ScalarValue::Int16(Some(*v)),
        Scalar::Byte(v) => ScalarValue::Int8(Some(*v)),
        Scalar::Float(v) => ScalarValue::Float32(Some(*v)),
        Scalar::Double(v) => ScalarValue::Float64(Some(*v)),
        Scalar::String(v) => ScalarValue::Utf8(Some(v.clone())),
        Scalar::Boolean(v) => ScalarValue::Boolean(Some(*v)),
        Scalar::Date(v) => ScalarValue::Date32(Some(*v)),
        Scalar::Timestamp(v) => ScalarValue::TimestampMicrosecond(Some(*v), Some(Arc::from("UTC"))),
        Scalar::TimestampNtz(v) => ScalarValue::TimestampMicrosecond(Some(*v), None),
        Scalar::Binary(v) => ScalarValue::Binary(Some(v.clone())),
        Scalar::Decimal(d) => {
            ScalarValue::Decimal128(Some(d.bits()), d.ty().precision(), d.ty().scale() as i8)
        }
        Scalar::Null(dt) => typed_null_to_df(dt)?,
        Scalar::Struct(v) => scalar_struct_value_to_df(v)?,
        Scalar::Array(_) | Scalar::Map(_) => {
            return Err(unsupported(format!(
                "expr_translator: complex literal scalar {:?} is not yet supported",
                scalar.data_type()
            )))
        }
    })
}

fn scalar_struct_value_to_df(struct_data: &StructData) -> Result<ScalarValue, DataFusionError> {
    let fields: Vec<ArrowField> = struct_data
        .fields()
        .iter()
        .map(|f| {
            f.try_into_arrow().map_err(|e| {
                unsupported(format!(
                    "expr_translator: struct literal field `{}` conversion failed: {e}",
                    f.name()
                ))
            })
        })
        .collect::<Result<_, _>>()?;
    let values = struct_data
        .values()
        .iter()
        .map(|v| scalar_value_to_df(v)?.to_array())
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ScalarValue::Struct(Arc::new(StructArray::new(
        fields.into(),
        values,
        None,
    ))))
}

fn typed_null_to_df(data_type: &DataType) -> Result<ScalarValue, DataFusionError> {
    if let DataType::Primitive(p) = data_type {
        return Ok(match p {
            PrimitiveType::Integer => ScalarValue::Int32(None),
            PrimitiveType::Long => ScalarValue::Int64(None),
            PrimitiveType::Short => ScalarValue::Int16(None),
            PrimitiveType::Byte => ScalarValue::Int8(None),
            PrimitiveType::Float => ScalarValue::Float32(None),
            PrimitiveType::Double => ScalarValue::Float64(None),
            PrimitiveType::String => ScalarValue::Utf8(None),
            PrimitiveType::Boolean => ScalarValue::Boolean(None),
            PrimitiveType::Date => ScalarValue::Date32(None),
            PrimitiveType::Timestamp => {
                ScalarValue::TimestampMicrosecond(None, Some(Arc::from("UTC")))
            }
            PrimitiveType::TimestampNtz => ScalarValue::TimestampMicrosecond(None, None),
            PrimitiveType::Binary => ScalarValue::Binary(None),
            PrimitiveType::Decimal(d) => {
                ScalarValue::Decimal128(None, d.precision(), d.scale() as i8)
            }
        });
    }

    let arrow_dt: ArrowDataType = data_type.try_into_arrow().map_err(|e| {
        unsupported(format!(
            "expr_translator: typed NULL conversion failed for {data_type:?}: {e}"
        ))
    })?;
    ScalarValue::try_from(&arrow_dt).map_err(|e| {
        unsupported(format!(
            "expr_translator: typed NULL for {data_type:?} is not supported by DataFusion: {e}"
        ))
    })
}

// === Identity-Transform helpers ===

/// Resolve a kernel [`ColumnName`] path against `input_schema` and return the Arrow
/// [`Fields`] of the resulting struct type.
fn lookup_struct_fields_via_path(
    input_schema: &ArrowSchema,
    path: &ColumnName,
) -> Result<ArrowFields, DataFusionError> {
    let segments = path.path();
    let Some((first, rest)) = segments.split_first() else {
        return Err(crate::error::plan_compilation(
            "Identity Transform input_path must have at least one segment",
        ));
    };
    let first_field = input_schema
        .fields()
        .iter()
        .find(|f| f.name() == first.as_str())
        .ok_or_else(|| {
            crate::error::plan_compilation(format!(
                "Identity Transform input_path root `{first}` not found in input schema"
            ))
        })?;
    let mut current = first_field.data_type();
    for segment in rest {
        let ArrowDataType::Struct(fields) = current else {
            return Err(crate::error::plan_compilation(format!(
                "Identity Transform input_path traverses non-struct field at `{segment}`"
            )));
        };
        let next = fields
            .iter()
            .find(|f| f.name() == segment.as_str())
            .ok_or_else(|| {
                crate::error::plan_compilation(format!(
                    "Identity Transform input_path segment `{segment}` not found in struct"
                ))
            })?;
        current = next.data_type();
    }
    match current {
        ArrowDataType::Struct(fields) => Ok(fields.clone()),
        other => Err(crate::error::plan_compilation(format!(
            "Identity Transform input_path must resolve to a struct, found {other:?}"
        ))),
    }
}

/// Rebuild a struct expression with target field names while preserving the parent null
/// bitmap. The naive `named_struct(get_field(col, "a"), ...)` produces all-present rows; we
/// wrap in `CASE WHEN col IS NOT NULL THEN rebuilt ELSE NULL END` to keep parent-NULL rows.
fn rebuild_struct_with_target_names(
    input_expr: Expr,
    source_fields: &ArrowFields,
    target_struct: &StructType,
) -> Result<Expr, DataFusionError> {
    let target_count = target_struct.fields().count();
    if source_fields.len() != target_count {
        return Err(crate::error::plan_compilation(format!(
            "Identity Transform field count mismatch: source struct has {} fields, target \
             projection schema has {target_count} fields",
            source_fields.len(),
        )));
    }
    let mut args: Vec<Expr> = Vec::with_capacity(target_count * 2);
    for (source_field, target_field) in source_fields.iter().zip(target_struct.fields()) {
        let child_expr = get_field(input_expr.clone(), source_field.name().to_string());
        let renamed = rebuild_field_for_target(
            child_expr,
            source_field.data_type(),
            target_field.data_type(),
        )?;
        args.push(lit(target_field.name().to_string()));
        args.push(renamed);
    }
    let rebuilt = named_struct(args);
    Ok(Expr::Case(Case::new(
        None,
        vec![(
            Box::new(Expr::IsNotNull(Box::new(input_expr))),
            Box::new(rebuilt),
        )],
        Some(Box::new(Expr::Literal(ScalarValue::Null, None))),
    )))
}

/// Reshape `base_expr` so its values use `target_dt`'s logical names while keeping the
/// physical values intact. The recursion mirrors the kernel's column-mapping rename
/// shape:
///
/// - Struct -> Struct: recurse via [`rebuild_struct_with_target_names`] (which emits
///   `CASE WHEN parent IS NOT NULL THEN named_struct(...) ELSE NULL END` over the
///   children).
/// - List / LargeList / FixedSizeList of any element -> Array: dispatch to
///   [`rebuild_list_with_target_element`] which wraps the list in an `array_transform`
///   lambda that recursively reshapes the element.
/// - Everything else (primitive, Map, mismatched outer kinds): pass `base_expr` through
///   unchanged. Primitive names flow through the parent's `named_struct` literal; Map
///   reshape isn't implemented yet (no kernel schema in the current test corpus needs
///   it; the engine's outer projection still produces the right top-level name via
///   `Expr::alias`).
fn rebuild_field_for_target(
    base_expr: Expr,
    source_dt: &ArrowDataType,
    target_dt: &DataType,
) -> Result<Expr, DataFusionError> {
    match (source_dt, target_dt) {
        (ArrowDataType::Struct(src_fields), DataType::Struct(tgt_struct)) => {
            rebuild_struct_with_target_names(base_expr, src_fields, tgt_struct.as_ref())
        }
        (
            ArrowDataType::List(src_elem)
            | ArrowDataType::LargeList(src_elem)
            | ArrowDataType::FixedSizeList(src_elem, _),
            DataType::Array(tgt_array),
        ) => rebuild_list_with_target_element(base_expr, src_elem, &tgt_array.element_type),
        _ => Ok(base_expr),
    }
}

/// Reshape every element of `list_expr` from `src_elem`'s physical names to
/// `tgt_elem_dt`'s logical names using `array_transform(list, x -> reshape(x))`.
///
/// Short-circuits to a bare `list_expr` when the element doesn't actually need renaming
/// (primitive elements, or nested types whose names already match) so we don't pay for an
/// extra higher-order-function eval on the hot path.
fn rebuild_list_with_target_element(
    list_expr: Expr,
    src_elem: &Arc<ArrowField>,
    tgt_elem_dt: &DataType,
) -> Result<Expr, DataFusionError> {
    if !field_needs_rename(src_elem.data_type(), tgt_elem_dt) {
        return Ok(list_expr);
    }
    let lambda_field = Arc::new(ArrowField::new(
        "x",
        src_elem.data_type().clone(),
        src_elem.is_nullable(),
    ));
    let lambda_var = Expr::LambdaVariable(LambdaVariable::new(
        "x".to_string(),
        Some(lambda_field),
    ));
    let body = rebuild_field_for_target(lambda_var, src_elem.data_type(), tgt_elem_dt)?;
    Ok(array_transform(list_expr, lambda(["x"], body)))
}

/// True when reshaping `source_dt` to `target_dt` would change a struct field name
/// somewhere inside the type tree. Walks the same shape as [`rebuild_field_for_target`]
/// so the answer matches what that helper would actually emit.
fn field_needs_rename(source_dt: &ArrowDataType, target_dt: &DataType) -> bool {
    match (source_dt, target_dt) {
        (ArrowDataType::Struct(src), DataType::Struct(tgt)) => {
            if src.len() != tgt.fields().count() {
                // Mismatch: surface at recurse-time so the caller's error message wins.
                return true;
            }
            src.iter().zip(tgt.fields()).any(|(s, t)| {
                s.name() != t.name() || field_needs_rename(s.data_type(), t.data_type())
            })
        }
        (
            ArrowDataType::List(src)
            | ArrowDataType::LargeList(src)
            | ArrowDataType::FixedSizeList(src, _),
            DataType::Array(tgt),
        ) => field_needs_rename(src.data_type(), &tgt.element_type),
        _ => false,
    }
}

/// Build a per-top-level-field projection that maps the underlying provider's raw arrow
/// schema (physical column-id names on column-mapping tables) into the kernel's logical
/// schema. Each output expression is rooted at `col(source.name)` and is reshaped
/// recursively via [`rebuild_field_for_target`] when the field has nested structs or
/// lists of structs whose names differ between source and target.
///
/// Returns one [`Expr`] per top-level kernel field, in order. Callers wrap each result
/// in an alias (and, if they care about Arrow field metadata, the stamp UDF) before
/// handing it to [`DataFrame::select`](datafusion::dataframe::DataFrame::select).
pub fn build_logical_projection(
    source: &ArrowSchema,
    target: &StructType,
) -> Result<Vec<Expr>, DataFusionError> {
    let source_count = source.fields().len();
    let target_count = target.fields().count();
    if source_count != target_count {
        return Err(crate::error::plan_compilation(format!(
            "build_logical_projection: source schema has {source_count} top-level field(s); \
             target schema has {target_count}"
        )));
    }
    source
        .fields()
        .iter()
        .zip(target.fields())
        .map(|(source_field, target_field)| {
            let base = Expr::Column(Column::new_unqualified(source_field.name()));
            rebuild_field_for_target(base, source_field.data_type(), target_field.data_type())
        })
        .collect()
}

fn binary(l: Expr, op: Operator, r: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(Box::new(l), op, Box::new(r)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use delta_kernel::expressions::{
        column_expr, ArrayData, BinaryPredicateOp, ColumnName, Expression as Expr_,
        Predicate as Pred, Scalar,
    };
    use delta_kernel::schema::{ArrayType, DataType, StructField, StructType};
    use rstest::rstest;

    use super::{kernel_expr_to_df_untyped, kernel_pred_to_df};

    fn lower_expr(e: Expr_) -> String {
        format!("{}", kernel_expr_to_df_untyped(&e).unwrap())
    }
    fn lower_pred(p: Pred) -> String {
        format!("{}", kernel_pred_to_df(&p).unwrap())
    }

    #[rstest]
    #[case::depth_2(Expr_::column(["add", "path"]), "get_field(add, Utf8(\"path\"))")]
    #[case::depth_3(
        Expr_::Column(ColumnName::new(["a", "b", "c"])),
        "get_field(get_field(a, Utf8(\"b\")), Utf8(\"c\"))"
    )]
    fn nested_column_lowers_to_get_field_chain(#[case] kernel: Expr_, #[case] expected: &str) {
        assert_eq!(lower_expr(kernel), expected);
    }

    #[rstest]
    #[case::i32(Expr_::literal(7i32), "Int32(7)")]
    #[case::i64(Expr_::literal(42i64), "Int64(42)")]
    #[case::string(Expr_::literal("abc"), "Utf8(\"abc\")")]
    #[case::bool(Expr_::literal(true), "Boolean(true)")]
    #[case::null_long(Expr_::null_literal(DataType::LONG), "Int64(NULL)")]
    fn translates_primitive_literals(#[case] kernel: Expr_, #[case] expected: &str) {
        assert_eq!(lower_expr(kernel), expected);
    }

    #[rstest]
    #[case::eq(column_expr!("x").eq(Expr_::literal(1i64)), "x = Int64(1)")]
    #[case::lt(column_expr!("x").lt(Expr_::literal(1i64)), "x < Int64(1)")]
    #[case::gt(column_expr!("x").gt(Expr_::literal(1i64)), "x > Int64(1)")]
    #[case::distinct(
        column_expr!("x").distinct(Expr_::literal(1i64)),
        "x IS DISTINCT FROM Int64(1)"
    )]
    fn translates_binary_predicates(#[case] kernel: Pred, #[case] expected: &str) {
        assert_eq!(lower_pred(kernel), expected);
    }

    #[test]
    fn translates_arithmetic_binary() {
        assert_eq!(
            lower_expr(column_expr!("a") + Expr_::literal(5i64)),
            "a + Int64(5)"
        );
    }

    #[test]
    fn translates_in_to_in_list() {
        let arr = ArrayData::try_new(
            ArrayType::new(DataType::LONG, false),
            vec![Scalar::Long(1), Scalar::Long(2), Scalar::Long(3)],
        )
        .unwrap();
        let p = Pred::binary(
            BinaryPredicateOp::In,
            column_expr!("x"),
            Expr_::literal(Scalar::Array(arr)),
        );
        assert_eq!(lower_pred(p), "x IN ([Int64(1), Int64(2), Int64(3)])");
    }

    #[test]
    fn translates_not_predicate() {
        assert_eq!(
            lower_pred(Pred::not(column_expr!("x").is_null())),
            "NOT x IS NULL"
        );
    }

    #[test]
    fn translates_junction_and_or_left_associative() {
        // AND: IsNull + Gt
        assert_eq!(
            lower_pred(Pred::and(
                column_expr!("a").is_null(),
                column_expr!("b").gt(Expr_::literal(5i64)),
            )),
            "a IS NULL AND b > Int64(5)"
        );
        // OR_from chain: ((a IS NULL OR b IS NULL) OR c IS NULL).
        assert_eq!(
            lower_pred(Pred::or_from([
                column_expr!("a").is_null(),
                column_expr!("b").is_null(),
                column_expr!("c").is_null(),
            ])),
            "a IS NULL OR b IS NULL OR c IS NULL"
        );
    }

    #[test]
    fn translates_if_to_case() {
        let kernel = Expr_::if_then_else(
            column_expr!("x").is_null(),
            Expr_::literal(0i64),
            column_expr!("x"),
        );
        assert_eq!(
            lower_expr(kernel),
            "CASE WHEN x IS NULL THEN Int64(0) ELSE x END"
        );
    }

    #[test]
    fn nested_if_lowers_to_nested_case() {
        // IF(a IS NULL, 0, IF(a > 100, 100, a)) -- clamp pattern
        let kernel = Expr_::if_then_else(
            column_expr!("a").is_null(),
            Expr_::literal(0i64),
            Expr_::if_then_else(
                column_expr!("a").gt(Expr_::literal(100i64)),
                Expr_::literal(100i64),
                column_expr!("a"),
            ),
        );
        assert_eq!(
            lower_expr(kernel),
            "CASE WHEN a IS NULL THEN Int64(0) ELSE \
             CASE WHEN a > Int64(100) THEN Int64(100) ELSE a END END"
        );
    }

    #[test]
    fn complex_nested_predicate_round_trips() {
        // (a + 1 > 5) AND NOT (b IS NULL)
        let p = Pred::and(
            (column_expr!("a") + Expr_::literal(1i64)).gt(Expr_::literal(5i64)),
            Pred::not(column_expr!("b").is_null()),
        );
        assert_eq!(lower_pred(p), "a + Int64(1) > Int64(5) AND NOT b IS NULL");
    }

    #[test]
    fn parse_json_translates_to_json_get_and_named_struct() {
        let output_schema = Arc::new(
            StructType::try_new(vec![StructField::nullable("numRecords", DataType::LONG)]).unwrap(),
        );
        let lowered = lower_expr(Expr_::parse_json(column_expr!("stats"), output_schema));
        assert!(lowered.contains("named_struct"), "{lowered}");
        assert!(lowered.contains("json_get_int"), "{lowered}");
    }

    #[test]
    fn opaque_predicate_returns_unsupported() {
        let err = kernel_pred_to_df(&Pred::unknown("mystery")).unwrap_err();
        assert!(format!("{err}").contains("Unknown"));
    }
}
