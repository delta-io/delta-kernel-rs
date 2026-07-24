//! Conversion from a kernel [`Expression`] to a DataFusion [`Expr`].
//!
//! Untyped: it maps an expression to its natural DataFusion shape with no target output field.
//! The input schema is taken only to fail fast on unresolvable column references.
//!
//! # Typed vs untyped conversion
//!
//! Conversion takes an optional `output_type`: the [`DataType`] the expression is expected to
//! produce, threaded down from the enclosing [`Project`](delta_kernel::plans) node's schema. Most
//! arms ignore it -- an expression is a nameless recipe whose natural DataFusion shape carries its
//! own type -- and lower identically whether or not a target is supplied. The struct-shaped arms
//! ([`Expression::Struct`], [`Expression::StructPatch`], [`Expression::MapToStruct`]) are the
//! exception: they build a struct whose field *names* and per-field *types* live only in the target
//! schema, so they require a `Some(DataType::Struct(..))` target and error without one.
//! [`kernel_to_df_expr`] is the public entry for the untyped case (`output_type = None`); the typed
//! `Project`-node compiler calls [`kernel_to_df_expr_typed`] with the node's declared field type.
//!
//! The input schema is always supplied, but only to fail fast: a column reference is validated
//! against it and then lowered to a `col(..)`/`get_field(..)` chain. DataFusion would resolve that
//! chain against the upstream schema during plan analysis regardless; validating here turns a
//! dangling reference into a clear error at conversion time instead of a late analysis failure.
//!
//! The variants group by why they are or are not lowerable:
//!
//! 1. **Natural type** (`Literal`, `Column`, `Binary`, `Variadic`): self-describing; lowered here,
//!    target type ignored.
//! 2. **Predicate** (`Predicate`): a boolean-valued predicate used as a value. Lowered by
//!    delegating to the `Predicate -> Expr` converter.
//! 3. **Struct-shaped** (`Struct`, `StructPatch`, `MapToStruct`): build a struct against the target
//!    schema, which supplies field names and per-field types. Error without a
//!    `Some(DataType::Struct(..))` target.
//! 4. **ParseJson**: carries its own `output_schema`, but DataFusion core has no stock JSON-string
//!    -> struct parser, so it needs a custom scalar UDF mirroring the kernel decoder, not yet wired
//!    up.
//! 5. **Terminal** (`Unary(ToJson)`, `Opaque`, `Unknown`): no faithful lowering. `ToJson` needs a
//!    UDF kernel has not wired; `Opaque` is engine-defined and understood only through its trait
//!    methods; `Unknown` has no semantics to lower (kernel forbids interpreting it).
//!
//! A free function rather than `impl TryFrom`: both types are foreign to this crate (orphan rule).
//!
//! # Divergence from the kernel Arrow evaluator
//!
//! The struct-shaped arms build with DataFusion's `named_struct`, which marks every output field
//! nullable and does not enforce a declared NOT NULL. The kernel Arrow evaluator instead stamps
//! each field's declared nullability from the output schema
//! (`evaluate_struct_expression`/`evaluate_struct_patch_expression`). The produced *values* agree;
//! only the field-nullability flag on the built struct type differs. Reconciling it to the declared
//! `Project.schema` is the job of the (not-yet-built) `Project`-node compiler, not this converter.

use datafusion::common::{Column, ScalarValue};
use datafusion::functions::core::expr_fn::{
    coalesce, get_field, get_field_path, named_struct, nullif,
};
use datafusion::functions_nested::expr_fn::make_array;
use datafusion::logical_expr::{binary_expr, cast, lit, Case, Expr, Operator};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::expressions::{
    BinaryExpression, BinaryExpressionOp, ColumnName, Expression, ExpressionRef,
    ExpressionStructPatch, MapToStructExpression, UnaryExpressionOp, VariadicExpression,
    VariadicExpressionOp,
};
use delta_kernel::schema::{DataType, PrimitiveType, StructField, StructType};
use delta_kernel::{DeltaResult, Error};

use crate::predicate::kernel_predicate_to_df_expr;
use crate::scalar::kernel_to_df_scalar;

/// Converts a kernel [`Expression`] into the equivalent DataFusion [`Expr`] with no target output
/// type, validating column references against `input_schema` (the name-resolution scope: a column
/// path must resolve through the nested structs of this schema).
///
/// This is the untyped entry point. The struct-shaped arms (`Struct`, `StructPatch`, `MapToStruct`)
/// require a target type and are unsupported here; use [`kernel_to_df_expr_typed`] to lower them.
///
/// # Errors
///
/// See [`kernel_to_df_expr_typed`]; this delegates to it with no target type.
pub fn kernel_to_df_expr(expr: &Expression, input_schema: &StructType) -> DeltaResult<Expr> {
    kernel_to_df_expr_typed(expr, input_schema, None)
}

/// Converts a kernel [`Expression`] into the equivalent DataFusion [`Expr`], targeting
/// `output_type` (the [`DataType`] the expression must produce) and validating column references
/// against `input_schema`.
///
/// `output_type` is consulted only by the struct-shaped arms (`Struct`, `StructPatch`,
/// `MapToStruct`), which build a struct whose field names and per-field types come from the target
/// [`DataType::Struct`]. All other arms lower to their natural DataFusion shape and ignore it,
/// matching the kernel Arrow evaluator, which validates (but does not cast) a value against its
/// target type.
///
/// # Errors
///
/// Returns an error for a column reference that does not resolve against `input_schema`; for a
/// struct-shaped arm whose `output_type` is not `Some(DataType::Struct(..))` or whose field count
/// does not match the target; and [`Error::unsupported`] for expressions with no faithful
/// DataFusion equivalent: engine-defined (`Opaque`) or opaque-to-both (`Unknown`) expressions, the
/// `ToJson` unary op, and `ParseJson` (which needs a custom parsing UDF). Also propagates any error
/// from converting an embedded predicate or a child scalar (e.g. an interval literal, which has no
/// Arrow representation).
pub fn kernel_to_df_expr_typed(
    expr: &Expression,
    input_schema: &StructType,
    output_type: Option<&DataType>,
) -> DeltaResult<Expr> {
    match expr {
        Expression::Literal(scalar) => Ok(lit(kernel_to_df_scalar(scalar)?)),
        Expression::Column(name) => kernel_column_to_df_expr(name, input_schema),
        Expression::Binary(binary) => kernel_binary_expr_to_df_expr(binary, input_schema),
        Expression::Variadic(variadic) => {
            kernel_variadic_to_df_expr(variadic, input_schema, output_type)
        }
        Expression::Predicate(pred) => kernel_predicate_to_df_expr(pred, input_schema),

        // Struct-shaped: build a struct against the target schema. Both need `output_type` to be
        // `Some(DataType::Struct(..))` -- `Struct` for the field names and per-child types,
        // `StructPatch` for the output field names/types its sparse edit does not itself carry.
        Expression::Struct(fields, nullability) => {
            kernel_struct_to_df_expr(fields, nullability.as_ref(), input_schema, output_type)
        }
        Expression::StructPatch(patch) => {
            kernel_struct_patch_to_df_expr(patch, input_schema, output_type)
        }
        Expression::MapToStruct(map_to_struct) => {
            kernel_map_to_struct_to_df_expr(map_to_struct, input_schema, output_type)
        }

        // TODO: wire up via a custom parsing UDF. DataFusion core has no stock typed JSON parser.
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
fn kernel_binary_expr_to_df_expr(
    binary: &BinaryExpression,
    input_schema: &StructType,
) -> DeltaResult<Expr> {
    let op = match binary.op {
        BinaryExpressionOp::Plus => Operator::Plus,
        BinaryExpressionOp::Minus => Operator::Minus,
        BinaryExpressionOp::Multiply => Operator::Multiply,
        BinaryExpressionOp::Divide => Operator::Divide,
    };
    let left = kernel_to_df_expr(&binary.left, input_schema)?;
    let right = kernel_to_df_expr(&binary.right, input_schema)?;
    Ok(binary_expr(left, op, right))
}

/// Lowers a variadic expression: `Coalesce` to `coalesce(..)` and `Array` to `make_array(..)`,
/// each over the converted arguments.
///
/// `Coalesce` is type-preserving -- every branch yields the coalesce's own result type -- so the
/// target `output_type` propagates unchanged to each argument. `Array` does not: kernel's array
/// expression carries no per-element type, so its elements are lowered untyped.
fn kernel_variadic_to_df_expr(
    variadic: &VariadicExpression,
    input_schema: &StructType,
    output_type: Option<&DataType>,
) -> DeltaResult<Expr> {
    let arg_output_type = match variadic.op {
        VariadicExpressionOp::Coalesce => output_type,
        VariadicExpressionOp::Array => None,
    };
    let args = variadic
        .exprs
        .iter()
        .map(|e| kernel_to_df_expr_typed(e, input_schema, arg_output_type))
        .collect::<DeltaResult<Vec<_>>>()?;
    Ok(match variadic.op {
        VariadicExpressionOp::Coalesce => coalesce(args),
        VariadicExpressionOp::Array => make_array(args),
    })
}

/// Requires `output_type` to be a struct type, returning its [`StructType`]. The struct-shaped arms
/// build a struct whose field names and per-field types live only in the target schema, so `None`
/// is unsupported here and a non-struct target is a malformed plan (an internal inconsistency,
/// since a struct-shaped expression should only ever target a struct). This mirrors the kernel
/// Arrow evaluator, which dispatches these arms only on `Some(DataType::Struct(..))`.
fn require_struct_output<'a>(
    output_type: Option<&'a DataType>,
    arm: &str,
) -> DeltaResult<&'a StructType> {
    match output_type {
        Some(DataType::Struct(target)) => Ok(target),
        None => Err(Error::unsupported(format!(
            "converting a {arm} expression requires a struct output type"
        ))),
        Some(other) => Err(Error::generic(format!(
            "a {arm} expression must target a struct type, but got {other:?}"
        ))),
    }
}

/// Wraps `body` (a `named_struct(..)` that always produces a present struct) so the whole struct
/// becomes NULL when `guard` evaluates to false or null: `CASE WHEN guard THEN body ELSE NULL END`.
/// This matches kernel's row-level struct-null mask (both `Struct`'s `values & nulls` and
/// `StructPatch`'s cloned source null buffer null the whole struct where `guard` is not true).
///
/// The else-branch is an untyped NULL literal, which DataFusion's CASE coercion promotes to the
/// `THEN` branch's struct type -- deliberately, so the two arms need not agree on field nullability
/// (a concrete struct-typed null would carry the target's declared nullability and force a
/// struct-vs-struct reconciliation `named_struct`, which is all-nullable, cannot satisfy).
fn struct_null_when_not(guard: Expr, body: Expr) -> Expr {
    Expr::Case(Case::new(
        None,
        vec![(Box::new(guard), Box::new(body))],
        Some(Box::new(lit(ScalarValue::Null))),
    ))
}

/// Lowers a struct constructor to a DataFusion `named_struct(..)`, drawing field names and
/// per-child target types from `output_type` (which must be `Some(DataType::Struct(..))`). Each
/// child is lowered against its target field type so nested struct-shaped children get the context
/// they need.
///
/// An optional `nullability` predicate makes the whole struct null where it is not strictly true,
/// lowered as `CASE WHEN <nullability> THEN named_struct(..) ELSE NULL END` -- matching kernel's
/// `values & nulls` mask (predicate false or null -> null struct).
///
/// # Errors
///
/// Returns an error when `output_type` is not a struct, when the child count does not match the
/// target field count (a hard error, mirroring the kernel evaluator), or from lowering any child or
/// the nullability predicate.
fn kernel_struct_to_df_expr(
    fields: &[ExpressionRef],
    nullability: Option<&ExpressionRef>,
    input_schema: &StructType,
    output_type: Option<&DataType>,
) -> DeltaResult<Expr> {
    let target = require_struct_output(output_type, "Struct")?;
    if fields.len() != target.num_fields() {
        return Err(Error::generic(format!(
            "Struct expression field count mismatch: {} fields in expression but {} in schema",
            fields.len(),
            target.num_fields()
        )));
    }
    let mut args = Vec::with_capacity(fields.len() * 2);
    for (child, field) in fields.iter().zip(target.fields()) {
        args.push(lit(field.name().to_string()));
        args.push(kernel_to_df_expr_typed(
            child,
            input_schema,
            Some(field.data_type()),
        )?);
    }
    let body = named_struct(args);
    match nullability {
        None => Ok(body),
        // The nullability field is a value-typed `Expression`, not a `Predicate`, so lower it
        // through the value converter. Propagate a BOOLEAN target so a type-aware child (e.g. a
        // Coalesce) lowers against the right type; a non-boolean result is caught later by
        // DataFusion type analysis, not here.
        Some(pred) => {
            let guard = kernel_to_df_expr_typed(pred, input_schema, Some(&DataType::BOOLEAN))?;
            Ok(struct_null_when_not(guard, body))
        }
    }
}

/// Pulls the next field from an output-schema field iterator, erroring if it is exhausted (the
/// patch produced more columns than the output schema declares).
fn next_output_field<'a>(
    fields: &mut impl Iterator<Item = &'a StructField>,
) -> DeltaResult<&'a StructField> {
    fields
        .next()
        .ok_or_else(|| Error::generic("StructPatch produced more fields than the output schema"))
}

/// Emits one newly-computed field into `args`: consumes the next output field for its name and
/// target type, lowers `expr` against that type (so a nested struct-shaped insertion gets its
/// context), and pushes the `[name, value]` pair.
fn emit_new_field<'a>(
    args: &mut Vec<Expr>,
    output_fields: &mut impl Iterator<Item = &'a StructField>,
    input_schema: &StructType,
    expr: &Expression,
) -> DeltaResult<()> {
    let field = next_output_field(output_fields)?;
    let value = kernel_to_df_expr_typed(expr, input_schema, Some(field.data_type()))?;
    args.push(lit(field.name().to_string()));
    args.push(value);
    Ok(())
}

/// Lowers a struct patch (a sparse `O(changes)` edit of an input struct) to a DataFusion
/// `named_struct(..)` rebuild, drawing output field names and types positionally from `output_type`
/// (which must be `Some(DataType::Struct(..))`).
///
/// The rebuild walks the same emission order as kernel's `evaluate_struct_patch_expression`:
/// prepended fields, then each input field in schema order (passed through unless the patch drops
/// or replaces it, followed by that field's insertions), then appended fields. Each emitted column
/// consumes the next output field, supplying its name and target type; a hard error results if the
/// counts do not line up exactly. Input fields are pulled from the struct at `patch.input_path`
/// (resolved against `input_schema`); insertion, prepend, and append expressions are lowered
/// against the top-level `input_schema`, matching the evaluator, which evaluates them against the
/// whole input batch rather than the selected struct.
///
/// A nested patch (`input_path` present) preserves the input struct's row-level nullness -- a null
/// input struct yields a null output struct -- via `CASE WHEN <input_path> IS NOT NULL THEN
/// named_struct(..) ELSE NULL END`, matching the evaluator, which clones the source struct's null
/// buffer. A top-level patch needs no such guard (its source is the never-null batch).
///
/// KNOWN DIVERGENCE: for a multi-segment `input_path` (e.g. `a.b`, produced by a doubly-nested
/// patch), the `IS NOT NULL` guard is on `get_field(a, "b")`, which DataFusion evaluates as NULL
/// when the *ancestor* `a` is null -- so the whole output nulls if any ancestor is null. The
/// evaluator instead reads only the leaf struct's own null buffer, ignoring ancestor nullness.
/// This converter's behavior is arguably the more correct one, but it diverges from the evaluator
/// on the (untested) ancestor-null case; reconcile if a multi-segment nested patch reaches
/// execution. See TODO tracking in the branch.
///
/// # Errors
///
/// Returns an error when `output_type` is not a struct; when `input_path` does not resolve to a
/// struct in `input_schema`; when the emitted column count does not match the output field count;
/// when a non-optional field patch names an input field that does not exist; or from lowering any
/// child expression.
fn kernel_struct_patch_to_df_expr(
    patch: &ExpressionStructPatch,
    input_schema: &StructType,
    output_type: Option<&DataType>,
) -> DeltaResult<Expr> {
    let target = require_struct_output(output_type, "StructPatch")?;

    // Resolve the input struct this patch operates on. `None` = operate on the top-level columns.
    let (source_struct, source_expr) = match patch.input_path() {
        None => (input_schema, None),
        Some(path) => {
            let field = input_schema.field_at(path)?;
            let DataType::Struct(source_struct) = field.data_type() else {
                return Err(Error::generic(format!(
                    "StructPatch input_path '{path}' does not resolve to a struct"
                )));
            };
            (
                source_struct.as_ref(),
                Some(kernel_column_to_df_expr(path, input_schema)?),
            )
        }
    };

    let mut output_fields = target.fields();
    let mut args = Vec::with_capacity(target.num_fields() * 2);

    // Prepends: new fields before any input field.
    for expr in &patch.prepended_fields {
        emit_new_field(&mut args, &mut output_fields, input_schema, expr)?;
    }

    let mut used_field_patches = 0usize;
    for input_field in source_struct.fields() {
        let name = input_field.name();
        let field_patch = patch.field_patches.get(name);

        // Passthrough: no patch entry, or an entry that keeps the input field. The passed-through
        // column keeps its input array, so it consumes an output slot only for its name (the
        // evaluator likewise discards the output type here).
        if field_patch.is_none_or(|fp| fp.keep_input) {
            let field = next_output_field(&mut output_fields)?;
            let value = match &source_expr {
                Some(base) => get_field(base.clone(), name.to_string()),
                None => Expr::Column(Column::new_unqualified(name)),
            };
            args.push(lit(field.name().to_string()));
            args.push(value);
        }

        // Insertions after this field's output position.
        if let Some(field_patch) = field_patch {
            for expr in &field_patch.insertions {
                emit_new_field(&mut args, &mut output_fields, input_schema, expr)?;
            }
            used_field_patches += 1;
        }
    }

    // Every non-optional field patch must have matched an input field.
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

    // Appends: new fields after all input fields and their insertions.
    for expr in &patch.appended_fields {
        emit_new_field(&mut args, &mut output_fields, input_schema, expr)?;
    }

    if output_fields.next().is_some() {
        return Err(Error::generic(
            "StructPatch produced fewer fields than the output schema",
        ));
    }

    let body = named_struct(args);
    Ok(match source_expr {
        // Top-level patch: source is the never-null batch, so no null guard.
        None => body,
        // Nested patch: a null input struct yields a null output struct.
        Some(base) => struct_null_when_not(base.is_not_null(), body),
    })
}

/// Lowers a `MapToStruct` (reshape a `Map<String, String>` into a struct by parsing each value into
/// its target field type) to a DataFusion `named_struct(..)` rebuild. Field names and per-field
/// types come from `output_type`, which must be `Some(DataType::Struct(..))` of primitive fields
/// (matching the kernel evaluator, which supports only primitive targets).
///
/// Each field extracts its value with `cast(get_field(map, name), T)`. For a numeric or temporal
/// type the raw value is first wrapped in `nullif(.., '')`, mapping an empty string to null before
/// the cast, so an empty string becomes null (kernel's `empty_string_partition_cast`) while an
/// unparseable value fails the cast (kernel's hard parse error). String and Binary keep the raw
/// value (empty is a valid empty string / empty bytes). A missing key or null value is already null
/// via `get_field`. The whole struct is nulled where the input map row is null via `CASE WHEN map
/// IS NULL THEN NULL`.
///
/// Boolean and Decimal targets are unsupported: arrow's cast accepts inputs kernel's parser rejects
/// (`"yes"`/`"1"` for bool; a differently-scaled decimal, which it silently rescales), so there is
/// no faithful cast lowering. They await the parsing UDF, as ParseJson does.
///
/// KNOWN DIVERGENCE: on a malformed map with duplicate keys, `get_field` takes the leftmost entry
/// while the kernel evaluator takes the rightmost. Spec-compliant writers never emit duplicate
/// keys. When `map_expr` is a nested column (e.g. `add.partitionValues`), the null-map guard also
/// nulls the struct when an ancestor is null -- see [`kernel_struct_patch_to_df_expr`].
///
/// # Errors
///
/// Returns an error when `output_type` is not a struct or has a non-primitive, Boolean, or Decimal
/// field, or from lowering the map expression.
fn kernel_map_to_struct_to_df_expr(
    map_to_struct: &MapToStructExpression,
    input_schema: &StructType,
    output_type: Option<&DataType>,
) -> DeltaResult<Expr> {
    let target = require_struct_output(output_type, "MapToStruct")?;
    let map = kernel_to_df_expr(&map_to_struct.map_expr, input_schema)?;

    let mut args = Vec::with_capacity(target.num_fields() * 2);
    for field in target.fields() {
        let DataType::Primitive(prim) = field.data_type() else {
            return Err(Error::unsupported(format!(
                "MapToStruct only supports primitive target types, but field '{}' is {:?}",
                field.name(),
                field.data_type()
            )));
        };
        let raw = get_field(map.clone(), field.name().to_string());
        let value = match prim {
            PrimitiveType::String | PrimitiveType::Binary => raw,
            // Arrow's cast is more lenient than kernel's parser here (it accepts "yes"/"1" as bool
            // and rescales decimals to the target scale), so there is no faithful cast lowering.
            PrimitiveType::Boolean | PrimitiveType::Decimal(_) => {
                return Err(Error::unsupported(format!(
                    "MapToStruct target field '{}' has type {:?}, which needs a parsing UDF",
                    field.name(),
                    field.data_type()
                )))
            }
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
        Array, ArrayRef, BooleanArray, Int32Array, Int64Array, MapBuilder, RecordBatch,
        StringArray, StringBuilder, StructArray,
    };
    use datafusion::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use datafusion::common::DFSchema;
    use datafusion::execution::context::SessionContext;
    use delta_kernel::expressions::{column_expr, Expression as Expr_};
    use delta_kernel::schema::{DataType, MapType, StructField, StructType};
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
        kernel_to_df_expr(&expr, &test_schema())
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

    #[test]
    fn embedded_predicate_delegates_to_predicate_converter() {
        let kernel = Expr_::Predicate(Box::new(column_expr!("b").is_null()));
        assert_eq!(lower(kernel), "b IS NULL");
    }

    /// A column reference that does not resolve against the input schema fails at conversion time,
    /// not later during DataFusion analysis. Covers each `field_at` failure mode.
    #[rstest]
    #[case::empty(Expr_::Column(ColumnName::new(Vec::<String>::new())))]
    #[case::unknown_root(Expr_::column(["nope"]))]
    #[case::unknown_nested(Expr_::column(["a", "b", "missing"]))]
    #[case::descend_into_non_struct(Expr_::column(["x", "y"]))]
    fn unresolved_column_is_an_error(#[case] kernel: Expr_) {
        kernel_to_df_expr(&kernel, &test_schema()).unwrap_err();
    }

    // === Typed struct-shaped arms ===

    use delta_kernel::expressions::ExpressionStructPatchBuilder;

    /// Lowers an expression against [`test_schema`] targeting `output_type` and renders it as a
    /// DataFusion `Display` string.
    fn lower_typed(expr: Expr_, output_type: &DataType) -> String {
        kernel_to_df_expr_typed(&expr, &test_schema(), Some(output_type))
            .unwrap()
            .to_string()
    }

    /// A two-field target struct `{ p: long, q: long }` for struct/patch tests.
    fn pq_struct() -> DataType {
        StructType::try_new([
            StructField::nullable("p", DataType::LONG),
            StructField::nullable("q", DataType::LONG),
        ])
        .unwrap()
        .into()
    }

    #[test]
    fn struct_lowers_to_named_struct_with_target_names() {
        let kernel = Expr_::struct_from([column_expr!("b"), Expr_::literal(1i64)]);
        assert_eq!(
            lower_typed(kernel, &pq_struct()),
            "named_struct(Utf8(\"p\"), b, Utf8(\"q\"), Int64(1))"
        );
    }

    #[test]
    fn nested_struct_recurses_with_child_target_names() {
        // Target: { outer: { p: long, q: long } }, value: struct(struct(b, 1)).
        let inner = Expr_::struct_from([column_expr!("b"), Expr_::literal(1i64)]);
        let kernel = Expr_::struct_from([inner]);
        let target: DataType = StructType::try_new([StructField::nullable("outer", pq_struct())])
            .unwrap()
            .into();
        assert_eq!(
            lower_typed(kernel, &target),
            "named_struct(Utf8(\"outer\"), named_struct(Utf8(\"p\"), b, Utf8(\"q\"), Int64(1)))"
        );
    }

    #[test]
    fn struct_with_nullability_wraps_in_case() {
        let kernel = Expr_::struct_with_nullability_from(
            [column_expr!("b"), Expr_::literal(1i64)],
            Expr_::Predicate(Box::new(column_expr!("x").is_not_null())),
        );
        // CASE WHEN <guard> THEN named_struct(..) ELSE NULL(typed) END. Kernel models IS NOT NULL
        // as Not(IsNull), so the guard renders as "NOT x IS NULL".
        let rendered = lower_typed(kernel, &pq_struct());
        assert!(
            rendered.starts_with("CASE WHEN NOT x IS NULL THEN named_struct("),
            "{rendered}"
        );
        assert!(rendered.ends_with("END"), "{rendered}");
    }

    #[test]
    fn struct_without_target_is_unsupported() {
        let kernel = Expr_::struct_from([column_expr!("b")]);
        kernel_to_df_expr(&kernel, &test_schema()).unwrap_err();
    }

    #[test]
    fn struct_arity_mismatch_is_an_error() {
        // Two children, one-field target.
        let kernel = Expr_::struct_from([column_expr!("b"), Expr_::literal(1i64)]);
        let target: DataType = StructType::try_new([StructField::nullable("p", DataType::LONG)])
            .unwrap()
            .into();
        kernel_to_df_expr_typed(&kernel, &test_schema(), Some(&target)).unwrap_err();
    }

    // === Struct patch ===

    /// Lowers a struct patch built by `build_patch` against `input_schema`, targeting
    /// `output_type`.
    fn lower_patch(
        patch: ExpressionStructPatch,
        input: &StructType,
        output_type: &DataType,
    ) -> String {
        let expr = Expr_::struct_patch(patch).unwrap();
        kernel_to_df_expr_typed(&expr, input, Some(output_type))
            .unwrap()
            .to_string()
    }

    /// Input schema for top-level patch tests: `{ a: long, b: long }`.
    fn ab_schema() -> StructType {
        StructType::try_new([
            StructField::nullable("a", DataType::LONG),
            StructField::nullable("b", DataType::LONG),
        ])
        .unwrap()
    }

    #[test]
    fn empty_top_level_patch_passes_all_fields_through() {
        let patch = ExpressionStructPatchBuilder::new().build().unwrap();
        assert_eq!(
            lower_patch(patch, &ab_schema(), &pq_struct()),
            // Passthrough columns keep their input names as values; output names come from target.
            "named_struct(Utf8(\"p\"), a, Utf8(\"q\"), b)"
        );
    }

    #[test]
    fn top_level_patch_replace_puts_expr_in_field_slot() {
        // Replace `a` with a literal; keep `b`.
        let patch = ExpressionStructPatchBuilder::new()
            .replace("a", Expr_::literal(7i64))
            .build()
            .unwrap();
        assert_eq!(
            lower_patch(patch, &ab_schema(), &pq_struct()),
            "named_struct(Utf8(\"p\"), Int64(7), Utf8(\"q\"), b)"
        );
    }

    #[test]
    fn top_level_patch_drop_removes_field() {
        // Drop `a`, keep `b`; target has one field.
        let patch = ExpressionStructPatchBuilder::new()
            .drop("a")
            .build()
            .unwrap();
        let target: DataType = StructType::try_new([StructField::nullable("q", DataType::LONG)])
            .unwrap()
            .into();
        assert_eq!(
            lower_patch(patch, &ab_schema(), &target),
            "named_struct(Utf8(\"q\"), b)"
        );
    }

    #[test]
    fn top_level_patch_prepend_and_append() {
        let patch = ExpressionStructPatchBuilder::new()
            .prepend(Expr_::literal(0i64))
            .append(Expr_::literal(9i64))
            .build()
            .unwrap();
        // Target: { first, a, b, last }.
        let target: DataType = StructType::try_new([
            StructField::nullable("first", DataType::LONG),
            StructField::nullable("a", DataType::LONG),
            StructField::nullable("b", DataType::LONG),
            StructField::nullable("last", DataType::LONG),
        ])
        .unwrap()
        .into();
        assert_eq!(
            lower_patch(patch, &ab_schema(), &target),
            "named_struct(Utf8(\"first\"), Int64(0), Utf8(\"a\"), a, Utf8(\"b\"), b, \
             Utf8(\"last\"), Int64(9))"
        );
    }

    #[test]
    fn top_level_patch_insert_after_field() {
        let patch = ExpressionStructPatchBuilder::new()
            .insert_after("a", Expr_::literal(5i64))
            .build()
            .unwrap();
        // Target: { a, inserted, b }.
        let target: DataType = StructType::try_new([
            StructField::nullable("a", DataType::LONG),
            StructField::nullable("inserted", DataType::LONG),
            StructField::nullable("b", DataType::LONG),
        ])
        .unwrap()
        .into();
        assert_eq!(
            lower_patch(patch, &ab_schema(), &target),
            "named_struct(Utf8(\"a\"), a, Utf8(\"inserted\"), Int64(5), Utf8(\"b\"), b)"
        );
    }

    #[test]
    fn nested_patch_wraps_in_null_guard_case() {
        // Input schema: { s: { a: long, b: long } }. Patch replaces s.a with a literal.
        let input = StructType::try_new([StructField::nullable("s", pq_input_struct())]).unwrap();
        let patch = ExpressionStructPatchBuilder::new_nested(["s"])
            .replace("a", Expr_::literal(7i64))
            .build()
            .unwrap();
        let rendered = lower_patch(patch, &input, &pq_struct());
        // Nested: CASE WHEN s IS NOT NULL THEN named_struct(...) ELSE NULL END. The passthrough of
        // `b` pulls from the nested struct via get_field(s, "b").
        assert!(
            rendered.starts_with("CASE WHEN s IS NOT NULL THEN named_struct("),
            "{rendered}"
        );
        assert!(rendered.contains("get_field(s, Utf8(\"b\"))"), "{rendered}");
        assert!(rendered.ends_with("END"), "{rendered}");
    }

    /// Input nested struct `{ a: long, b: long }` for nested-patch tests.
    fn pq_input_struct() -> DataType {
        StructType::try_new([
            StructField::nullable("a", DataType::LONG),
            StructField::nullable("b", DataType::LONG),
        ])
        .unwrap()
        .into()
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
        let expr = Expr_::struct_patch(patch).unwrap();
        kernel_to_df_expr_typed(&expr, &ab_schema(), Some(&target)).unwrap_err();
    }

    #[test]
    fn patch_too_few_output_fields_is_an_error() {
        // Empty patch passes 2 fields; target declares 1.
        let patch = ExpressionStructPatchBuilder::new().build().unwrap();
        let target: DataType = StructType::try_new([StructField::nullable("p", DataType::LONG)])
            .unwrap()
            .into();
        let expr = Expr_::struct_patch(patch).unwrap();
        kernel_to_df_expr_typed(&expr, &ab_schema(), Some(&target)).unwrap_err();
    }

    #[test]
    fn patch_without_target_is_unsupported() {
        let patch = ExpressionStructPatchBuilder::new().build().unwrap();
        let expr = Expr_::struct_patch(patch).unwrap();
        kernel_to_df_expr(&expr, &ab_schema()).unwrap_err();
    }

    #[test]
    fn patch_non_struct_target_is_an_error() {
        let patch = ExpressionStructPatchBuilder::new().build().unwrap();
        let expr = Expr_::struct_patch(patch).unwrap();
        kernel_to_df_expr_typed(&expr, &ab_schema(), Some(&DataType::LONG)).unwrap_err();
    }

    #[test]
    fn required_patch_on_missing_field_is_an_error() {
        // A non-optional patch (replace) naming a field the input schema lacks must error.
        let patch = ExpressionStructPatchBuilder::new()
            .replace("nonexistent", Expr_::literal(1i64))
            .build()
            .unwrap();
        let expr = Expr_::struct_patch(patch).unwrap();
        kernel_to_df_expr_typed(&expr, &ab_schema(), Some(&pq_struct())).unwrap_err();
    }

    #[test]
    fn optional_patch_on_missing_field_is_tolerated() {
        // An optional patch (drop_if_exists) on a missing field is silently ignored: all input
        // fields pass through unchanged.
        let patch = ExpressionStructPatchBuilder::new()
            .drop_if_exists("nonexistent")
            .build()
            .unwrap();
        assert_eq!(
            lower_patch(patch, &ab_schema(), &pq_struct()),
            "named_struct(Utf8(\"p\"), a, Utf8(\"q\"), b)"
        );
    }

    // === Execution: prove the nullability CASE actually resolves and masks the right rows ===

    /// Builds a physical expr from a lowered kernel expression against `batch`'s schema (which
    /// runs DataFusion's simplification + type coercion -- the step Display-string assertions
    /// cannot exercise), evaluates it, and returns the resulting array.
    fn eval_against(expr: Expr, batch: &RecordBatch) -> ArrayRef {
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

    /// A struct with a nullability predicate resolves through DataFusion coercion (the CASE's
    /// `named_struct` THEN branch and untyped-NULL ELSE branch reconcile to a struct type) and
    /// masks the whole struct to null exactly where the predicate is false.
    #[test]
    fn struct_with_nullability_masks_struct_to_null() {
        // input `keep: boolean` = [true, false]; target { p: long } with value struct(lit 1),
        // nullability = the `keep` column, so row 0 is present and row 1 is a null struct.
        let input =
            StructType::try_new([StructField::nullable("keep", DataType::BOOLEAN)]).unwrap();
        let kernel =
            Expr_::struct_with_nullability_from([Expr_::literal(1i64)], column_expr!("keep"));
        let target: DataType = StructType::try_new([StructField::nullable("p", DataType::LONG)])
            .unwrap()
            .into();
        let expr = kernel_to_df_expr_typed(&kernel, &input, Some(&target)).unwrap();

        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![ArrowField::new(
                "keep",
                ArrowDataType::Boolean,
                true,
            )])),
            vec![Arc::new(BooleanArray::from(vec![true, false]))],
        )
        .unwrap();

        let result = eval_against(expr, &batch);
        let structs = result.as_any().downcast_ref::<StructArray>().unwrap();
        assert!(
            structs.is_valid(0),
            "row 0 (keep=true) should be a present struct"
        );
        assert!(
            structs.is_null(1),
            "row 1 (keep=false) should be a null struct"
        );
    }

    /// A nested struct patch resolves through coercion and nulls the output struct exactly where
    /// the source struct row is null (matching the evaluator's cloned source null buffer).
    #[test]
    fn nested_patch_masks_output_to_null_when_source_struct_is_null() {
        // Input { s: { a: long } }; patch on `s` replaces `a` with a literal. Row 1 has a null `s`.
        let input = StructType::try_new([StructField::nullable(
            "s",
            StructType::try_new([StructField::nullable("a", DataType::LONG)]).unwrap(),
        )])
        .unwrap();
        let patch = ExpressionStructPatchBuilder::new_nested(["s"])
            .replace("a", Expr_::literal(7i64))
            .build()
            .unwrap();
        let target: DataType = StructType::try_new([StructField::nullable("a", DataType::LONG)])
            .unwrap()
            .into();
        let expr =
            kernel_to_df_expr_typed(&Expr_::struct_patch(patch).unwrap(), &input, Some(&target))
                .unwrap();

        // s = [ {a: 10}, null ] -- row 1's struct is null (via the null buffer).
        let s = StructArray::try_new(
            vec![ArrowField::new("a", ArrowDataType::Int64, true)].into(),
            vec![Arc::new(Int64Array::from(vec![Some(10), Some(0)])) as ArrayRef],
            Some(vec![true, false].into()),
        )
        .unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![ArrowField::new(
                "s",
                s.data_type().clone(),
                true,
            )])),
            vec![Arc::new(s)],
        )
        .unwrap();

        let result = eval_against(expr, &batch);
        let structs = result.as_any().downcast_ref::<StructArray>().unwrap();
        assert!(
            structs.is_valid(0),
            "row 0 (s present) should be a present struct"
        );
        assert!(structs.is_null(1), "row 1 (s null) should be a null struct");
    }

    // === Arms still unsupported ===

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
    fn region_id_struct() -> DataType {
        StructType::try_new([
            StructField::nullable("region", DataType::STRING),
            StructField::nullable("id", DataType::INTEGER),
        ])
        .unwrap()
        .into()
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

    /// Lowers a `MapToStruct` over `pv` targeting `output_type`, evaluates it against `batch`, and
    /// returns the resulting struct array.
    fn eval_map_to_struct(output_type: &DataType, batch: &RecordBatch) -> StructArray {
        let kernel = Expr_::map_to_struct(column_expr!("pv"));
        let expr = kernel_to_df_expr_typed(&kernel, &pv_map_schema(), Some(output_type)).unwrap();
        eval_against(expr, batch)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .clone()
    }

    #[test]
    fn map_to_struct_lowers_to_named_struct_over_get_field() {
        let kernel = Expr_::map_to_struct(column_expr!("pv"));
        let rendered =
            kernel_to_df_expr_typed(&kernel, &pv_map_schema(), Some(&region_id_struct()))
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
        let kernel = Expr_::map_to_struct(column_expr!("pv"));
        kernel_to_df_expr(&kernel, &pv_map_schema()).unwrap_err();
    }

    #[test]
    fn map_to_struct_non_struct_target_is_an_error() {
        let kernel = Expr_::map_to_struct(column_expr!("pv"));
        kernel_to_df_expr_typed(&kernel, &pv_map_schema(), Some(&DataType::LONG)).unwrap_err();
    }

    #[test]
    fn map_to_struct_non_primitive_field_is_unsupported() {
        let target: DataType = StructType::try_new([StructField::nullable("nested", pq_struct())])
            .unwrap()
            .into();
        let kernel = Expr_::map_to_struct(column_expr!("pv"));
        kernel_to_df_expr_typed(&kernel, &pv_map_schema(), Some(&target)).unwrap_err();
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
        let target: DataType = StructType::try_new([StructField::nullable("f", field_type)])
            .unwrap()
            .into();
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
        let kernel = Expr_::map_to_struct(column_expr!("pv"));
        let expr = kernel_to_df_expr_typed(&kernel, &pv_map_schema(), Some(&target)).unwrap();
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
        let target: DataType =
            StructType::try_new([StructField::nullable("id", DataType::INTEGER)])
                .unwrap()
                .into();
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
        use datafusion::arrow::array::{Date32Array, TimestampMicrosecondArray};
        let target: DataType = StructType::try_new([
            StructField::nullable("d", DataType::DATE),
            StructField::nullable("ts", DataType::TIMESTAMP),
        ])
        .unwrap()
        .into();
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

    #[rstest]
    // Arrow's cast is more lenient than kernel's parser, so these have no faithful lowering.
    #[case::boolean(DataType::BOOLEAN)]
    #[case::decimal(DataType::decimal(5, 2).unwrap())]
    fn map_to_struct_lenient_cast_types_are_unsupported(#[case] field_type: DataType) {
        let target: DataType = StructType::try_new([StructField::nullable("f", field_type)])
            .unwrap()
            .into();
        let kernel = Expr_::map_to_struct(column_expr!("pv"));
        kernel_to_df_expr_typed(&kernel, &pv_map_schema(), Some(&target)).unwrap_err();
    }
}
