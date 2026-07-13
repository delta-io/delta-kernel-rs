//! Defines [`KernelExpressionVisitorState`]. This is a visitor that can be used to convert an
//! engine's native expressions into kernel's [`Expression`] and [`Predicate`] types.
use std::sync::Arc;

#[cfg(feature = "default-engine-base")]
use delta_kernel::engine::arrow_expression::opaque::ArrowOpaquePredicate;
use delta_kernel::expressions::{
    BinaryExpressionOp, BinaryPredicateOp, ColumnName, Expression, JunctionPredicateOp, Predicate,
    Scalar, UnaryPredicateOp,
};
use delta_kernel::schema::{DataType, PrimitiveType};
use delta_kernel::DeltaResult;

#[cfg(feature = "default-engine-base")]
use crate::expressions::opaque_eval::{COpaqueEvalCallbacks, FfiOpaqueEvalCallbacks};
#[cfg(feature = "default-engine-base")]
use crate::expressions::FfiOpaquePredicateOp;
use crate::expressions::{SharedExpression, SharedPredicate};
use crate::handle::Handle;
use crate::scan::{EngineExpression, EnginePredicate};
use crate::{
    AllocateErrorFn, EngineIterator, ExternResult, IntoExternResult, KernelStringSlice,
    ReferenceSet, TryFromStringSlice,
};

pub(crate) enum ExpressionOrPredicate {
    Expression(Expression),
    Predicate(Predicate),
}

#[derive(Default)]
pub struct KernelExpressionVisitorState {
    inflight_ids: ReferenceSet<ExpressionOrPredicate>,
}

fn wrap_expression(state: &mut KernelExpressionVisitorState, expr: impl Into<Expression>) -> usize {
    let expr = ExpressionOrPredicate::Expression(expr.into());
    state.inflight_ids.insert(expr)
}

fn wrap_predicate(state: &mut KernelExpressionVisitorState, pred: impl Into<Predicate>) -> usize {
    let pred = ExpressionOrPredicate::Predicate(pred.into());
    state.inflight_ids.insert(pred)
}

pub(crate) fn unwrap_kernel_expression(
    state: &mut KernelExpressionVisitorState,
    exprid: usize,
) -> Option<Expression> {
    match state.inflight_ids.take(exprid)? {
        ExpressionOrPredicate::Expression(expr) => Some(expr),
        ExpressionOrPredicate::Predicate(pred) => Some(Expression::from_pred(pred)),
    }
}

pub(crate) fn unwrap_kernel_predicate(
    state: &mut KernelExpressionVisitorState,
    predid: usize,
) -> Option<Predicate> {
    match state.inflight_ids.take(predid)? {
        ExpressionOrPredicate::Expression(expr) => Some(Predicate::from_expr(expr)),
        ExpressionOrPredicate::Predicate(pred) => Some(pred),
    }
}

fn visit_expression_binary(
    state: &mut KernelExpressionVisitorState,
    op: BinaryExpressionOp,
    a: usize,
    b: usize,
) -> usize {
    let a = unwrap_kernel_expression(state, a);
    let b = unwrap_kernel_expression(state, b);
    match (a, b) {
        (Some(a), Some(b)) => wrap_expression(state, Expression::binary(op, a, b)),
        _ => 0, // invalid child => invalid node
    }
}

fn visit_predicate_binary(
    state: &mut KernelExpressionVisitorState,
    op: BinaryPredicateOp,
    a: usize,
    b: usize,
) -> usize {
    let a = unwrap_kernel_expression(state, a);
    let b = unwrap_kernel_expression(state, b);
    match (a, b) {
        (Some(a), Some(b)) => wrap_predicate(state, Predicate::binary(op, a, b)),
        _ => 0, // invalid child => invalid node
    }
}

fn visit_predicate_unary(
    state: &mut KernelExpressionVisitorState,
    op: UnaryPredicateOp,
    inner_expr: usize,
) -> usize {
    unwrap_kernel_expression(state, inner_expr)
        .map_or(0, |expr| wrap_predicate(state, Predicate::unary(op, expr)))
}

// The EngineIterator is not thread safe, not reentrant, not owned by callee, not freed by callee.
#[no_mangle]
pub extern "C" fn visit_predicate_and(
    state: &mut KernelExpressionVisitorState,
    children: &mut EngineIterator,
) -> usize {
    let result = Predicate::and_from(
        children.flat_map(|child| unwrap_kernel_predicate(state, child as usize)),
    );
    wrap_predicate(state, result)
}

#[no_mangle]
pub extern "C" fn visit_expression_plus(
    state: &mut KernelExpressionVisitorState,
    a: usize,
    b: usize,
) -> usize {
    visit_expression_binary(state, BinaryExpressionOp::Plus, a, b)
}

#[no_mangle]
pub extern "C" fn visit_expression_minus(
    state: &mut KernelExpressionVisitorState,
    a: usize,
    b: usize,
) -> usize {
    visit_expression_binary(state, BinaryExpressionOp::Minus, a, b)
}

#[no_mangle]
pub extern "C" fn visit_expression_multiply(
    state: &mut KernelExpressionVisitorState,
    a: usize,
    b: usize,
) -> usize {
    visit_expression_binary(state, BinaryExpressionOp::Multiply, a, b)
}

#[no_mangle]
pub extern "C" fn visit_expression_divide(
    state: &mut KernelExpressionVisitorState,
    a: usize,
    b: usize,
) -> usize {
    visit_expression_binary(state, BinaryExpressionOp::Divide, a, b)
}

#[no_mangle]
pub extern "C" fn visit_predicate_lt(
    state: &mut KernelExpressionVisitorState,
    a: usize,
    b: usize,
) -> usize {
    visit_predicate_binary(state, BinaryPredicateOp::LessThan, a, b)
}

#[no_mangle]
pub extern "C" fn visit_predicate_le(
    state: &mut KernelExpressionVisitorState,
    a: usize,
    b: usize,
) -> usize {
    let p = visit_predicate_binary(state, BinaryPredicateOp::GreaterThan, a, b);
    visit_predicate_not(state, p)
}

#[no_mangle]
pub extern "C" fn visit_predicate_gt(
    state: &mut KernelExpressionVisitorState,
    a: usize,
    b: usize,
) -> usize {
    visit_predicate_binary(state, BinaryPredicateOp::GreaterThan, a, b)
}

#[no_mangle]
pub extern "C" fn visit_predicate_ge(
    state: &mut KernelExpressionVisitorState,
    a: usize,
    b: usize,
) -> usize {
    let p = visit_predicate_binary(state, BinaryPredicateOp::LessThan, a, b);
    visit_predicate_not(state, p)
}

#[no_mangle]
pub extern "C" fn visit_predicate_eq(
    state: &mut KernelExpressionVisitorState,
    a: usize,
    b: usize,
) -> usize {
    visit_predicate_binary(state, BinaryPredicateOp::Equal, a, b)
}

#[no_mangle]
pub extern "C" fn visit_predicate_ne(
    state: &mut KernelExpressionVisitorState,
    a: usize,
    b: usize,
) -> usize {
    let p = visit_predicate_binary(state, BinaryPredicateOp::Equal, a, b);
    visit_predicate_not(state, p)
}

#[no_mangle]
pub extern "C" fn visit_predicate_unknown(
    state: &mut KernelExpressionVisitorState,
    name: KernelStringSlice,
) -> usize {
    let name = unsafe { TryFromStringSlice::try_from_slice(&name) };
    name.map_or(0, |name| wrap_predicate(state, Predicate::Unknown(name)))
}

#[no_mangle]
pub extern "C" fn visit_expression_unknown(
    state: &mut KernelExpressionVisitorState,
    name: KernelStringSlice,
) -> usize {
    let name = unsafe { TryFromStringSlice::try_from_slice(&name) };
    name.map_or(0, |name| wrap_expression(state, Expression::Unknown(name)))
}

/// # Safety
/// The string slice must be valid
#[no_mangle]
pub unsafe extern "C" fn visit_expression_column(
    state: &mut KernelExpressionVisitorState,
    name: KernelStringSlice,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name = unsafe { TryFromStringSlice::try_from_slice(&name) };
    visit_expression_column_impl(state, name).into_extern_result(&allocate_error)
}
fn visit_expression_column_impl(
    state: &mut KernelExpressionVisitorState,
    name: DeltaResult<&str>,
) -> DeltaResult<usize> {
    // TODO: FIXME: This is incorrect if any field name in the column path contains a period.
    let name = ColumnName::from_naive_str_split(name?);
    Ok(wrap_expression(state, name))
}

#[no_mangle]
pub extern "C" fn visit_predicate_not(
    state: &mut KernelExpressionVisitorState,
    inner_pred: usize,
) -> usize {
    unwrap_kernel_predicate(state, inner_pred)
        .map_or(0, |pred| wrap_predicate(state, Predicate::not(pred)))
}

#[no_mangle]
pub extern "C" fn visit_predicate_is_null(
    state: &mut KernelExpressionVisitorState,
    inner_expr: usize,
) -> usize {
    visit_predicate_unary(state, UnaryPredicateOp::IsNull, inner_expr)
}

/// # Safety
/// The string slice must be valid
#[no_mangle]
pub unsafe extern "C" fn visit_expression_literal_string(
    state: &mut KernelExpressionVisitorState,
    value: KernelStringSlice,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let value = unsafe { String::try_from_slice(&value) };
    visit_expression_literal_string_impl(state, value).into_extern_result(&allocate_error)
}
fn visit_expression_literal_string_impl(
    state: &mut KernelExpressionVisitorState,
    value: DeltaResult<String>,
) -> DeltaResult<usize> {
    Ok(wrap_expression(state, Expression::literal(value?)))
}

// We need to get parse.expand working to be able to macro everything below, see issue #255
#[no_mangle]
pub extern "C" fn visit_expression_literal_int(
    state: &mut KernelExpressionVisitorState,
    value: i32,
) -> usize {
    wrap_expression(state, Expression::literal(value))
}

#[no_mangle]
pub extern "C" fn visit_expression_literal_long(
    state: &mut KernelExpressionVisitorState,
    value: i64,
) -> usize {
    wrap_expression(state, Expression::literal(value))
}

#[no_mangle]
pub extern "C" fn visit_expression_literal_short(
    state: &mut KernelExpressionVisitorState,
    value: i16,
) -> usize {
    wrap_expression(state, Expression::literal(value))
}

#[no_mangle]
pub extern "C" fn visit_expression_literal_byte(
    state: &mut KernelExpressionVisitorState,
    value: i8,
) -> usize {
    wrap_expression(state, Expression::literal(value))
}

#[no_mangle]
pub extern "C" fn visit_expression_literal_float(
    state: &mut KernelExpressionVisitorState,
    value: f32,
) -> usize {
    wrap_expression(state, Expression::literal(value))
}

#[no_mangle]
pub extern "C" fn visit_expression_literal_double(
    state: &mut KernelExpressionVisitorState,
    value: f64,
) -> usize {
    wrap_expression(state, Expression::literal(value))
}

#[no_mangle]
pub extern "C" fn visit_expression_literal_bool(
    state: &mut KernelExpressionVisitorState,
    value: bool,
) -> usize {
    wrap_expression(state, Expression::literal(value))
}

/// visit a date literal expression 'value' (i32 representing days since unix epoch)
#[no_mangle]
pub extern "C" fn visit_expression_literal_date(
    state: &mut KernelExpressionVisitorState,
    value: i32,
) -> usize {
    wrap_expression(state, Expression::literal(Scalar::Date(value)))
}

/// visit a timestamp literal expression 'value' (i64 representing microseconds since unix epoch)
#[no_mangle]
pub extern "C" fn visit_expression_literal_timestamp(
    state: &mut KernelExpressionVisitorState,
    value: i64,
) -> usize {
    wrap_expression(state, Expression::literal(Scalar::Timestamp(value)))
}

/// visit a timestamp_ntz literal expression 'value' (i64 representing microseconds since unix
/// epoch)
#[no_mangle]
pub extern "C" fn visit_expression_literal_timestamp_ntz(
    state: &mut KernelExpressionVisitorState,
    value: i64,
) -> usize {
    wrap_expression(state, Expression::literal(Scalar::TimestampNtz(value)))
}

/// visit a binary literal expression
///
/// # Safety
/// The caller must ensure that `value` points to a valid array of at least `len` bytes.
#[no_mangle]
pub unsafe extern "C" fn visit_expression_literal_binary(
    state: &mut KernelExpressionVisitorState,
    value: *const u8,
    len: usize,
) -> usize {
    let bytes = std::slice::from_raw_parts(value, len);
    wrap_expression(state, Expression::literal(Scalar::Binary(bytes.to_vec())))
}

/// visit a decimal literal expression
///
/// Returns an error if the precision/scale combination is invalid.
#[no_mangle]
pub extern "C" fn visit_expression_literal_decimal(
    state: &mut KernelExpressionVisitorState,
    value_hi: u64,
    value_lo: u64,
    precision: u8,
    scale: u8,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    // SAFETY: The allocate_error function pointer is provided by the engine and assumed valid.
    unsafe {
        visit_expression_literal_decimal_impl(state, value_hi, value_lo, precision, scale)
            .into_extern_result(&allocate_error)
    }
}

fn visit_expression_literal_decimal_impl(
    state: &mut KernelExpressionVisitorState,
    value_hi: u64,
    value_lo: u64,
    precision: u8,
    scale: u8,
) -> DeltaResult<usize> {
    // Reconstruct the i128 from two u64 parts
    let value = ((value_hi as i128) << 64) | (value_lo as i128);
    let decimal = Scalar::decimal(value, precision, scale)?;
    Ok(wrap_expression(state, Expression::literal(decimal)))
}

/// Type tag for null literal construction via FFI. Identifies the data type of a typed null.
///
/// Primitive types use fixed discriminants 0-11. Decimal uses 12 and requires additional
/// precision/scale parameters. Non-primitive types (struct, array, map, variant) use the
/// [`NonPrimitive`](Self::NonPrimitive) sentinel -- these cannot be reconstructed from a type
/// tag alone.
///
/// NOTE: These values are part of the FFI contract. Changing existing discriminants is a breaking
/// change.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NullTypeTag {
    /// Null of type `boolean`.
    Boolean = 0,
    /// Null of type `byte` (8-bit signed integer).
    Byte = 1,
    /// Null of type `short` (16-bit signed integer).
    Short = 2,
    /// Null of type `integer` (32-bit signed integer).
    Integer = 3,
    /// Null of type `long` (64-bit signed integer).
    Long = 4,
    /// Null of type `float` (32-bit IEEE 754).
    Float = 5,
    /// Null of type `double` (64-bit IEEE 754).
    Double = 6,
    /// Null of type `string`.
    String = 7,
    /// Null of type `binary`.
    Binary = 8,
    /// Null of type `date` (days since epoch).
    Date = 9,
    /// Null of type `timestamp` (microseconds since epoch, UTC-adjusted).
    Timestamp = 10,
    /// Null of type `timestamp_ntz` (microseconds since epoch, no timezone).
    TimestampNtz = 11,
    /// Null of type `decimal`. Requires valid `precision` and `scale` parameters.
    ///
    /// WARNING: This variant MUST remain `= 12`. It is the only tag with special handling
    /// (precision/scale parameters), and C consumers key on the value `12` directly.
    Decimal = 12,
    /// Sentinel for non-primitive null types (struct, array, map, variant). Emitted by the
    /// kernel-to-engine visitor when the null's type is not a primitive. Engines that receive
    /// this tag should use opaque expressions or a schema visitor to obtain full type details.
    ///
    /// Passing this tag to [`visit_expression_literal_null`] returns an error because the
    /// original complex type cannot be reconstructed from a tag alone.
    NonPrimitive = 255,
}

impl TryFrom<u8> for NullTypeTag {
    type Error = delta_kernel::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Boolean),
            1 => Ok(Self::Byte),
            2 => Ok(Self::Short),
            3 => Ok(Self::Integer),
            4 => Ok(Self::Long),
            5 => Ok(Self::Float),
            6 => Ok(Self::Double),
            7 => Ok(Self::String),
            8 => Ok(Self::Binary),
            9 => Ok(Self::Date),
            10 => Ok(Self::Timestamp),
            11 => Ok(Self::TimestampNtz),
            12 => Ok(Self::Decimal),
            255 => Ok(Self::NonPrimitive),
            other => Err(delta_kernel::Error::generic(format!(
                "Unrecognized null type tag: {other}"
            ))),
        }
    }
}

impl NullTypeTag {
    /// Convert a [`DataType`] to its null type tag and decimal parameters.
    ///
    /// Returns `(tag, precision, scale)`. The `precision` and `scale` are only meaningful when
    /// `tag` is [`NullTypeTag::Decimal`]; they are zero for all other types.
    pub(crate) fn from_data_type(data_type: &DataType) -> (Self, u8, u8) {
        match data_type {
            DataType::Primitive(p) => match p {
                PrimitiveType::Boolean => (Self::Boolean, 0, 0),
                PrimitiveType::Byte => (Self::Byte, 0, 0),
                PrimitiveType::Short => (Self::Short, 0, 0),
                PrimitiveType::Integer => (Self::Integer, 0, 0),
                PrimitiveType::Long => (Self::Long, 0, 0),
                PrimitiveType::Float => (Self::Float, 0, 0),
                PrimitiveType::Double => (Self::Double, 0, 0),
                PrimitiveType::String => (Self::String, 0, 0),
                PrimitiveType::Binary => (Self::Binary, 0, 0),
                PrimitiveType::Date => (Self::Date, 0, 0),
                PrimitiveType::Timestamp => (Self::Timestamp, 0, 0),
                PrimitiveType::TimestampNtz => (Self::TimestampNtz, 0, 0),
                PrimitiveType::IntervalYearMonth | PrimitiveType::IntervalDayTime => {
                    // No FFI null tag for intervals; the sentinel avoids a new discriminant
                    (Self::NonPrimitive, 0, 0)
                }
                PrimitiveType::Decimal(dt) => (Self::Decimal, dt.precision(), dt.scale()),
                // Void has no dedicated FFI tag. The current predicate-construction path is
                // not expected to produce a void-typed literal null; if one ever reaches this
                // code we want it represented as a non-primitive null rather than a missing
                // case, so this arm is a defensive fallback rather than part of the normal
                // void-column path.
                PrimitiveType::Void => (Self::NonPrimitive, 0, 0),
                // Geometry/Geography carry an SRID string (and for Geography, an edge
                // algorithm) that cannot fit in the (tag, u8, u8) payload. Like intervals
                // above, they fall back to the sentinel so a reconstruction attempt errors
                // rather than silently mistyping the null; the SRID is not recoverable from
                // the tag. See #2914 for adding real geo FFI support.
                PrimitiveType::Geometry(_) | PrimitiveType::Geography(_) => {
                    (Self::NonPrimitive, 0, 0)
                }
            },
            _ => (Self::NonPrimitive, 0, 0),
        }
    }

    /// Convert a null type tag back to a [`DataType`].
    ///
    /// For [`Decimal`](Self::Decimal), `precision` and `scale` specify the decimal parameters.
    /// For all other primitive types, `precision` and `scale` are ignored (callers should
    /// pass 0).
    ///
    /// Returns an error for [`NonPrimitive`](Self::NonPrimitive) since complex types cannot be
    /// reconstructed from a type tag alone.
    pub(crate) fn to_data_type(self, precision: u8, scale: u8) -> DeltaResult<DataType> {
        match self {
            Self::Boolean => Ok(DataType::BOOLEAN),
            Self::Byte => Ok(DataType::BYTE),
            Self::Short => Ok(DataType::SHORT),
            Self::Integer => Ok(DataType::INTEGER),
            Self::Long => Ok(DataType::LONG),
            Self::Float => Ok(DataType::FLOAT),
            Self::Double => Ok(DataType::DOUBLE),
            Self::String => Ok(DataType::STRING),
            Self::Binary => Ok(DataType::BINARY),
            Self::Date => Ok(DataType::DATE),
            Self::Timestamp => Ok(DataType::TIMESTAMP),
            Self::TimestampNtz => Ok(DataType::TIMESTAMP_NTZ),
            Self::Decimal => Ok(DataType::Primitive(PrimitiveType::decimal(
                precision, scale,
            )?)),
            Self::NonPrimitive => Err(delta_kernel::Error::generic(
                "Non-primitive null types (struct, array, map, variant) cannot be reconstructed \
                 from a type tag. Use opaque expressions or a schema visitor instead.",
            )),
        }
    }
}

/// Visit a typed null literal expression.
///
/// The `type_tag` identifies the data type using the `NullTypeTag` encoding. For decimal nulls
/// (`type_tag == 12`), `precision` and `scale` specify the decimal type parameters; for all
/// other types, callers should pass 0 for both.
///
/// Returns an error if the type tag is unrecognized, if the tag is `NonPrimitive` (255), or
/// if the decimal precision/scale is invalid.
#[no_mangle]
pub extern "C" fn visit_expression_literal_null(
    state: &mut KernelExpressionVisitorState,
    type_tag: u8,
    precision: u8,
    scale: u8,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    // SAFETY: The allocate_error function pointer is provided by the engine and assumed valid.
    unsafe {
        visit_expression_literal_null_impl(state, type_tag, precision, scale)
            .into_extern_result(&allocate_error)
    }
}

fn visit_expression_literal_null_impl(
    state: &mut KernelExpressionVisitorState,
    type_tag: u8,
    precision: u8,
    scale: u8,
) -> DeltaResult<usize> {
    let tag = NullTypeTag::try_from(type_tag)?;
    let data_type = tag.to_data_type(precision, scale)?;
    Ok(wrap_expression(
        state,
        Expression::literal(Scalar::Null(data_type)),
    ))
}

#[no_mangle]
pub extern "C" fn visit_predicate_distinct(
    state: &mut KernelExpressionVisitorState,
    a: usize,
    b: usize,
) -> usize {
    visit_predicate_binary(state, BinaryPredicateOp::Distinct, a, b)
}

#[no_mangle]
pub extern "C" fn visit_predicate_in(
    state: &mut KernelExpressionVisitorState,
    a: usize,
    b: usize,
) -> usize {
    visit_predicate_binary(state, BinaryPredicateOp::In, a, b)
}

#[no_mangle]
pub extern "C" fn visit_predicate_or(
    state: &mut KernelExpressionVisitorState,
    children: &mut EngineIterator,
) -> usize {
    let result = Predicate::junction(
        JunctionPredicateOp::Or,
        children.flat_map(|child| unwrap_kernel_predicate(state, child as usize)),
    );
    wrap_predicate(state, result)
}

#[no_mangle]
pub extern "C" fn visit_expression_struct(
    state: &mut KernelExpressionVisitorState,
    children: &mut EngineIterator,
) -> usize {
    let exprs: Vec<Expression> = children
        .flat_map(|child| unwrap_kernel_expression(state, child as usize))
        .collect();
    wrap_expression(state, Expression::struct_from(exprs))
}

/// Visit a MapToStruct expression. The `child_expr` is the map expression.
#[no_mangle]
pub extern "C" fn visit_expression_map_to_struct(
    state: &mut KernelExpressionVisitorState,
    child_expr: usize,
) -> usize {
    unwrap_kernel_expression(state, child_expr).map_or(0, |expr| {
        wrap_expression(state, Expression::map_to_struct(expr))
    })
}

/// Convert an engine expression to a kernel expression using the visitor
/// pattern.
///
/// # Safety
///
/// Caller must ensure that `engine_expression` points to a valid
/// `EngineExpression` with a valid visitor function and expression pointer.
#[no_mangle]
pub unsafe extern "C" fn visit_engine_expression(
    engine_expression: &mut EngineExpression,
    allocate_error: AllocateErrorFn,
) -> ExternResult<Handle<SharedExpression>> {
    visit_engine_expression_impl(engine_expression).into_extern_result(&allocate_error)
}

fn visit_engine_expression_impl(
    engine_expression: &mut EngineExpression,
) -> DeltaResult<Handle<SharedExpression>> {
    let mut visitor_state = KernelExpressionVisitorState::default();
    let expr_id = (engine_expression.visitor)(engine_expression.expression, &mut visitor_state);

    let expr = unwrap_kernel_expression(&mut visitor_state, expr_id).ok_or_else(|| {
        delta_kernel::Error::generic(format!(
            "Invalid expression ID {expr_id} returned from engine visitor"
        ))
    })?;

    Ok(Arc::new(expr).into())
}

/// Convert an engine predicate to a kernel predicate using the visitor
/// pattern.
///
/// # Safety
///
/// Caller must ensure that `engine_predicate` points to a valid
/// `EnginePredicate` with a valid visitor function and predicate pointer.
#[no_mangle]
pub unsafe extern "C" fn visit_engine_predicate(
    engine_predicate: &mut EnginePredicate,
    allocate_error: AllocateErrorFn,
) -> ExternResult<Handle<SharedPredicate>> {
    visit_engine_predicate_impl(engine_predicate).into_extern_result(&allocate_error)
}

fn visit_engine_predicate_impl(
    engine_predicate: &mut EnginePredicate,
) -> DeltaResult<Handle<SharedPredicate>> {
    let mut visitor_state = KernelExpressionVisitorState::default();
    let pred_id = (engine_predicate.visitor)(engine_predicate.predicate, &mut visitor_state);

    let pred = unwrap_kernel_predicate(&mut visitor_state, pred_id).ok_or_else(|| {
        delta_kernel::Error::generic(format!(
            "Invalid predicate ID {pred_id} returned from engine visitor"
        ))
    })?;

    Ok(Arc::new(pred).into())
}

// === Opaque predicate / expression builders ====================================

/// Drain `children` and resolve each ID to an [`Expression`]. Returns `None`
/// if any ID is invalid (already consumed, never created, or zero); on
/// failure all remaining valid IDs are still drained from state.
fn resolve_opaque_children<I: IntoIterator<Item = usize>>(
    state: &mut KernelExpressionVisitorState,
    child_ids: I,
) -> Option<Vec<Expression>> {
    // Drain ALL ids first so the ReferenceSet is consistently emptied,
    // even when an earlier id is invalid.
    let resolved: Vec<Option<Expression>> = child_ids
        .into_iter()
        .map(|id| unwrap_kernel_expression(state, id))
        .collect();
    resolved.into_iter().collect()
}

/// Build a placeholder for an engine-defined predicate that kernel cannot evaluate: a NULL
/// boolean literal, which abstains from all pruning (even under `NOT`) while sibling predicates
/// prune normally. `children` are drained and discarded.
///
/// Use [`visit_predicate_opaque_with_eval`] to attach engine eval callbacks so the node itself
/// can participate in file pruning.
///
/// Returns 0 if any child ID is invalid.
///
/// # Safety
/// `name` must be valid UTF-8 for the duration of the call.
#[no_mangle]
pub unsafe extern "C" fn visit_predicate_opaque(
    state: &mut KernelExpressionVisitorState,
    name: KernelStringSlice,
    children: &mut EngineIterator,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name = unsafe { String::try_from_slice(&name) };
    visit_predicate_opaque_impl(state, name, children).into_extern_result(&allocate_error)
}

fn visit_predicate_opaque_impl(
    state: &mut KernelExpressionVisitorState,
    name: DeltaResult<String>,
    children: &mut EngineIterator,
) -> DeltaResult<usize> {
    let name = name?;
    if resolve_opaque_children(state, children.map(|c| c as usize)).is_none() {
        return Ok(0);
    }
    tracing::info!("opaque predicate `{name}`: no eval callbacks; kernel will not prune on it");
    Ok(wrap_predicate(state, Predicate::null_literal()))
}

/// Build an opaque predicate over `FfiOpaquePredicateOp(name, callbacks)` and
/// `children`, routed through the default engine's Arrow batch evaluator (via
/// `Predicate::arrow_opaque`). Kernel pre-evaluates each child arg recursively
/// via its standard `evaluate_expression`, exports the resulting columns as a
/// single `RecordBatch` over Arrow C Data Interface, and invokes the engine's
/// eval callback. Engine never walks the AST.
///
/// `callbacks` is passed by value; ownership of its `engine_state` transfers to
/// kernel, which invokes `free_state` exactly once -- even when this call fails
/// or returns 0. Engines attaching the same logical state to multiple opaque
/// ops must pass independently freeable state per call.
///
/// Returns 0 if any child ID is invalid.
///
/// # Safety
/// `name` must be valid UTF-8 for the duration of the call; the function
/// pointers in `callbacks` must remain valid for the lifetime of the built
/// predicate.
#[cfg(feature = "default-engine-base")]
#[no_mangle]
pub unsafe extern "C" fn visit_predicate_opaque_with_eval(
    state: &mut KernelExpressionVisitorState,
    name: KernelStringSlice,
    children: &mut EngineIterator,
    callbacks: COpaqueEvalCallbacks,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let name = unsafe { String::try_from_slice(&name) };
    // Wrap immediately so free_state fires exactly once on every exit path.
    let callbacks = Arc::new(FfiOpaqueEvalCallbacks::new(callbacks));
    visit_predicate_opaque_with_eval_impl(state, name, children, callbacks)
        .into_extern_result(&allocate_error)
}

#[cfg(feature = "default-engine-base")]
fn visit_predicate_opaque_with_eval_impl(
    state: &mut KernelExpressionVisitorState,
    name: DeltaResult<String>,
    children: &mut EngineIterator,
    callbacks: Arc<FfiOpaqueEvalCallbacks>,
) -> DeltaResult<usize> {
    let name = name?;
    let Some(exprs) = resolve_opaque_children(state, children.map(|c| c as usize)) else {
        return Ok(0);
    };
    let op = FfiOpaquePredicateOp::new(name, callbacks);
    Ok(wrap_predicate(state, Predicate::arrow_opaque(op, exprs)))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use delta_kernel::expressions::{Expression, Scalar};
    use delta_kernel::schema::{ArrayType, DataType, MapType, StructField, StructType};
    use rstest::rstest;

    use super::*;

    // ============================================================================
    // NullTypeTag::from_data_type
    // ============================================================================

    #[rstest]
    #[case(DataType::BOOLEAN, NullTypeTag::Boolean, 0, 0)]
    #[case(DataType::BYTE, NullTypeTag::Byte, 0, 0)]
    #[case(DataType::SHORT, NullTypeTag::Short, 0, 0)]
    #[case(DataType::INTEGER, NullTypeTag::Integer, 0, 0)]
    #[case(DataType::LONG, NullTypeTag::Long, 0, 0)]
    #[case(DataType::FLOAT, NullTypeTag::Float, 0, 0)]
    #[case(DataType::DOUBLE, NullTypeTag::Double, 0, 0)]
    #[case(DataType::STRING, NullTypeTag::String, 0, 0)]
    #[case(DataType::BINARY, NullTypeTag::Binary, 0, 0)]
    #[case(DataType::DATE, NullTypeTag::Date, 0, 0)]
    #[case(DataType::TIMESTAMP, NullTypeTag::Timestamp, 0, 0)]
    #[case(DataType::TIMESTAMP_NTZ, NullTypeTag::TimestampNtz, 0, 0)]
    fn from_data_type_primitive(
        #[case] dt: DataType,
        #[case] expected_tag: NullTypeTag,
        #[case] expected_precision: u8,
        #[case] expected_scale: u8,
    ) {
        let (tag, p, s) = NullTypeTag::from_data_type(&dt);
        assert_eq!(tag, expected_tag);
        assert_eq!(p, expected_precision);
        assert_eq!(s, expected_scale);
    }

    #[test]
    fn from_data_type_decimal() {
        let dt = DataType::decimal(18, 5).unwrap();
        let (tag, p, s) = NullTypeTag::from_data_type(&dt);
        assert_eq!(tag, NullTypeTag::Decimal);
        assert_eq!(p, 18);
        assert_eq!(s, 5);
    }

    #[test]
    fn from_data_type_non_primitive_produces_sentinel() {
        let struct_type = DataType::from(
            StructType::try_new(vec![StructField::not_null("a", DataType::INTEGER)]).unwrap(),
        );
        assert_eq!(
            NullTypeTag::from_data_type(&struct_type),
            (NullTypeTag::NonPrimitive, 0, 0)
        );

        let array_type = DataType::from(ArrayType::new(DataType::INTEGER, false));
        assert_eq!(
            NullTypeTag::from_data_type(&array_type),
            (NullTypeTag::NonPrimitive, 0, 0)
        );

        let map_type = DataType::from(MapType::new(DataType::STRING, DataType::INTEGER, false));
        assert_eq!(
            NullTypeTag::from_data_type(&map_type),
            (NullTypeTag::NonPrimitive, 0, 0)
        );

        let variant_type = DataType::unshredded_variant();
        assert_eq!(
            NullTypeTag::from_data_type(&variant_type),
            (NullTypeTag::NonPrimitive, 0, 0)
        );
    }

    // ============================================================================
    // NullTypeTag::to_data_type
    // ============================================================================

    #[rstest]
    #[case(NullTypeTag::Boolean, DataType::BOOLEAN)]
    #[case(NullTypeTag::Byte, DataType::BYTE)]
    #[case(NullTypeTag::Short, DataType::SHORT)]
    #[case(NullTypeTag::Integer, DataType::INTEGER)]
    #[case(NullTypeTag::Long, DataType::LONG)]
    #[case(NullTypeTag::Float, DataType::FLOAT)]
    #[case(NullTypeTag::Double, DataType::DOUBLE)]
    #[case(NullTypeTag::String, DataType::STRING)]
    #[case(NullTypeTag::Binary, DataType::BINARY)]
    #[case(NullTypeTag::Date, DataType::DATE)]
    #[case(NullTypeTag::Timestamp, DataType::TIMESTAMP)]
    #[case(NullTypeTag::TimestampNtz, DataType::TIMESTAMP_NTZ)]
    fn to_data_type_primitive(#[case] tag: NullTypeTag, #[case] expected: DataType) {
        assert_eq!(tag.to_data_type(0, 0).unwrap(), expected);
    }

    #[test]
    fn to_data_type_decimal() {
        let dt = NullTypeTag::Decimal.to_data_type(10, 2).unwrap();
        assert_eq!(dt, DataType::decimal(10, 2).unwrap());
    }

    #[test]
    fn to_data_type_decimal_invalid_precision_returns_error() {
        assert!(NullTypeTag::Decimal.to_data_type(0, 0).is_err());
    }

    #[test]
    fn to_data_type_decimal_scale_exceeds_precision_returns_error() {
        assert!(NullTypeTag::Decimal.to_data_type(5, 10).is_err());
    }

    #[test]
    fn to_data_type_non_primitive_returns_error() {
        let result = NullTypeTag::NonPrimitive.to_data_type(0, 0);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("Non-primitive"), "unexpected error: {msg}");
    }

    // ============================================================================
    // TryFrom<u8>
    // ============================================================================

    #[rstest]
    #[case(0, NullTypeTag::Boolean)]
    #[case(1, NullTypeTag::Byte)]
    #[case(2, NullTypeTag::Short)]
    #[case(3, NullTypeTag::Integer)]
    #[case(4, NullTypeTag::Long)]
    #[case(5, NullTypeTag::Float)]
    #[case(6, NullTypeTag::Double)]
    #[case(7, NullTypeTag::String)]
    #[case(8, NullTypeTag::Binary)]
    #[case(9, NullTypeTag::Date)]
    #[case(10, NullTypeTag::Timestamp)]
    #[case(11, NullTypeTag::TimestampNtz)]
    #[case(12, NullTypeTag::Decimal)]
    #[case(255, NullTypeTag::NonPrimitive)]
    fn try_from_u8_valid(#[case] value: u8, #[case] expected: NullTypeTag) {
        assert_eq!(NullTypeTag::try_from(value).unwrap(), expected);
    }

    #[rstest]
    #[case(13)]
    #[case(42)]
    #[case(254)]
    fn try_from_u8_invalid(#[case] value: u8) {
        let result = NullTypeTag::try_from(value);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("Unrecognized null type tag"),
            "unexpected error: {msg}"
        );
    }

    // ============================================================================
    // visit_expression_literal_null_impl (error paths)
    // ============================================================================

    #[test]
    fn visit_null_decimal_invalid_precision_returns_error() {
        let mut state = KernelExpressionVisitorState::default();
        assert!(visit_expression_literal_null_impl(&mut state, 12, 0, 0).is_err());
    }

    #[test]
    fn visit_null_unrecognized_tag_returns_error() {
        let mut state = KernelExpressionVisitorState::default();
        assert!(visit_expression_literal_null_impl(&mut state, 13, 0, 0).is_err());
    }

    #[test]
    fn visit_null_non_primitive_tag_returns_error() {
        let mut state = KernelExpressionVisitorState::default();
        assert!(visit_expression_literal_null_impl(&mut state, 255, 0, 0).is_err());
    }

    // ============================================================================
    // Round-trip: from_data_type -> (tag as u8, p, s) -> visit_expression_literal_null_impl
    // ============================================================================

    #[rstest]
    #[case(DataType::BOOLEAN)]
    #[case(DataType::BYTE)]
    #[case(DataType::SHORT)]
    #[case(DataType::INTEGER)]
    #[case(DataType::LONG)]
    #[case(DataType::FLOAT)]
    #[case(DataType::DOUBLE)]
    #[case(DataType::STRING)]
    #[case(DataType::BINARY)]
    #[case(DataType::DATE)]
    #[case(DataType::TIMESTAMP)]
    #[case(DataType::TIMESTAMP_NTZ)]
    fn null_type_round_trips_through_tag_encoding(#[case] data_type: DataType) {
        let (tag, precision, scale) = NullTypeTag::from_data_type(&data_type);
        let mut state = KernelExpressionVisitorState::default();
        let id =
            visit_expression_literal_null_impl(&mut state, tag as u8, precision, scale).unwrap();
        let expr = unwrap_kernel_expression(&mut state, id).unwrap();
        assert_eq!(expr, Expression::literal(Scalar::Null(data_type)),);
    }

    #[test]
    fn decimal_null_round_trips_through_tag_encoding() {
        let dt = DataType::decimal(20, 3).unwrap();
        let (tag, precision, scale) = NullTypeTag::from_data_type(&dt);
        assert_eq!(tag, NullTypeTag::Decimal);
        let mut state = KernelExpressionVisitorState::default();
        let id =
            visit_expression_literal_null_impl(&mut state, tag as u8, precision, scale).unwrap();
        let expr = unwrap_kernel_expression(&mut state, id).unwrap();
        assert_eq!(expr, Expression::literal(Scalar::Null(dt)));
    }

    // ============================================================================
    // Opaque-op builders (visit_predicate_opaque_impl and friends)
    //
    // These tests drive the `_impl` functions directly so we can construct
    // `EngineIterator` from a plain Vec without going through the C FFI.
    // ============================================================================

    use std::os::raw::c_void;
    use std::ptr::NonNull;

    /// Backing buffer for a test `EngineIterator`: yields each id as a pointer (consumer casts back
    /// via `c as usize`). An id of 0 reads as end-of-iteration, fine since 0 is kernel's "invalid
    /// id".
    struct IterState {
        ids: Vec<usize>,
        idx: usize,
    }

    extern "C" fn iter_next_direct(data: NonNull<c_void>) -> *const c_void {
        // SAFETY: `data` was set up by `make_iter` to point at a live
        // `IterState`; the iterator is single-threaded and never re-entered.
        let state = unsafe { &mut *(data.as_ptr() as *mut IterState) };
        if state.idx >= state.ids.len() {
            return std::ptr::null();
        }
        let id = state.ids[state.idx];
        state.idx += 1;
        id as *const c_void
    }

    /// RAII wrapper that frees the heap-allocated `IterState` backing an `EngineIterator`. Held as
    /// a raw pointer via `Box::into_raw` so stacked borrows don't retag it while `it` holds the
    /// pointer.
    struct IterStateBox(*mut IterState);

    impl Drop for IterStateBox {
        fn drop(&mut self) {
            // SAFETY: produced by Box::into_raw in make_iter; reclaimed exactly once on drop.
            drop(unsafe { Box::from_raw(self.0) });
        }
    }

    fn make_iter(ids: Vec<usize>) -> (IterStateBox, EngineIterator) {
        let raw = Box::into_raw(Box::new(IterState { ids, idx: 0 }));
        let it = EngineIterator {
            data: NonNull::new(raw as *mut c_void).unwrap(),
            get_next: iter_next_direct,
        };
        (IterStateBox(raw), it)
    }

    fn make_two_literal_ids(state: &mut KernelExpressionVisitorState) -> (usize, usize) {
        let a = wrap_expression(state, Expression::literal(1i32));
        let b = wrap_expression(state, Expression::literal(2i32));
        (a, b)
    }

    #[test]
    fn visit_predicate_opaque_impl_builds_null_literal_placeholder() {
        let mut state = KernelExpressionVisitorState::default();
        let (a, b) = make_two_literal_ids(&mut state);
        let (_keep, mut it) = make_iter(vec![a, b]);
        let id = visit_predicate_opaque_impl(&mut state, Ok("MY_OP".to_string()), &mut it).unwrap();
        assert_ne!(id, 0);
        let pred = unwrap_kernel_predicate(&mut state, id).unwrap();
        // No eval callbacks => NULL boolean literal, which abstains everywhere (even under NOT).
        assert_eq!(pred, Predicate::null_literal());
        // Children are drained from the visitor state even though they're discarded.
        assert!(state.inflight_ids.is_empty());
    }

    #[test]
    fn visit_predicate_opaque_returns_zero_on_invalid_child() {
        // 9999 is a never-issued id, so the builder bails without wrapping.
        let mut state = KernelExpressionVisitorState::default();
        let (_keep, mut it) = make_iter(vec![9999usize]);
        let id = visit_predicate_opaque_impl(&mut state, Ok("OP".to_string()), &mut it).unwrap();
        assert_eq!(id, 0, "invalid child should produce id=0 (no node created)");
    }

    // ============================================================================
    // Top-level `extern "C"` entry points. These drive the real FFI symbols (with
    // an engine-supplied `allocate_error` and the handle alloc/dealloc dance) so
    // miri can validate the unsafe boundary, not just the `_impl` helpers above.
    // ============================================================================

    use crate::ffi_test_utils::{allocate_err, ok_or_panic};
    use crate::kernel_string_slice;

    #[test]
    fn visit_predicate_opaque_ffi_builds_null_literal_placeholder() {
        let mut state = KernelExpressionVisitorState::default();
        let (a, b) = make_two_literal_ids(&mut state);
        let (_keep, mut it) = make_iter(vec![a, b]);
        let name = "MY_OP";
        let result = unsafe {
            visit_predicate_opaque(
                &mut state,
                kernel_string_slice!(name),
                &mut it,
                allocate_err,
            )
        };
        let id = ok_or_panic(result);
        assert_ne!(id, 0);
        let pred = unwrap_kernel_predicate(&mut state, id).unwrap();
        assert_eq!(pred, Predicate::null_literal());
    }

    /// Drives `visit_predicate_opaque_with_eval` end to end: pass the callbacks struct by value,
    /// build the predicate through the FFI symbol, then verify the engine's `free_state` fires
    /// exactly once when the predicate drops -- and also fires when the build bails early (invalid
    /// child), since ownership of `engine_state` transfers with the call.
    #[cfg(feature = "default-engine-base")]
    #[test]
    fn visit_predicate_opaque_with_eval_ffi_builds_and_frees() {
        use std::ffi::c_void;
        use std::sync::atomic::{AtomicUsize, Ordering};

        use crate::engine_data::ArrowFFIData;
        use crate::error::EngineExecResult;

        static FREED: AtomicUsize = AtomicUsize::new(0);

        unsafe extern "C" fn stub_eval(
            _: *mut c_void,
            _: KernelStringSlice,
            _: ArrowFFIData,
            _: bool,
            _: *mut EngineExecResult<ArrowFFIData>,
        ) {
        }
        unsafe extern "C" fn counting_free(_: *mut c_void) {
            FREED.fetch_add(1, Ordering::SeqCst);
        }
        fn callbacks() -> COpaqueEvalCallbacks {
            COpaqueEvalCallbacks {
                engine_state: std::ptr::null_mut(),
                eval_pred_rows: stub_eval,
                eval_pred_stats: stub_eval,
                free_state: counting_free,
            }
        }

        FREED.store(0, Ordering::SeqCst);
        let mut state = KernelExpressionVisitorState::default();
        let (a, b) = make_two_literal_ids(&mut state);
        let (_keep, mut it) = make_iter(vec![a, b]);
        let name = "MY_EVAL_OP";
        let result = unsafe {
            visit_predicate_opaque_with_eval(
                &mut state,
                kernel_string_slice!(name),
                &mut it,
                callbacks(),
                allocate_err,
            )
        };
        let id = ok_or_panic(result);
        assert_ne!(id, 0);
        assert_eq!(
            FREED.load(Ordering::SeqCst),
            0,
            "predicate still holds the callbacks"
        );

        // Dropping the predicate drops the last ref, firing the engine's free_state exactly once.
        let pred = unwrap_kernel_predicate(&mut state, id).unwrap();
        drop(pred);
        assert_eq!(FREED.load(Ordering::SeqCst), 1, "free_state must fire once");

        // Bail-early path: an invalid child returns id 0, but ownership already transferred, so
        // free_state still fires.
        let (_keep, mut bad_it) = make_iter(vec![9999usize]);
        let id = ok_or_panic(unsafe {
            visit_predicate_opaque_with_eval(
                &mut state,
                kernel_string_slice!(name),
                &mut bad_it,
                callbacks(),
                allocate_err,
            )
        });
        assert_eq!(id, 0);
        assert_eq!(
            FREED.load(Ordering::SeqCst),
            2,
            "free_state must fire on the bail-early path too"
        );
    }

    /// End to end: build an opaque `IN_RANGE(id, 25)` predicate through the
    /// `visit_predicate_opaque_with_eval` FFI symbol, then run a real `DefaultEngine` scan over a
    /// 3-file in-memory table with disjoint `id` ranges. The engine callback prunes files whose
    /// [min, max] excludes 25, so only the file covering [20, 30] survives -- proving the symbol ->
    /// rewrite -> StatsMode dispatch -> file pruning chain works against a real log.
    // Gated on a TLS backend (not bare `default-engine-base`): this test builds a real
    // `DefaultEngineBuilder`, and kernel cannot compile with `default-engine-base` alone.
    #[cfg(any(
        feature = "default-engine-rustls",
        feature = "default-engine-native-tls"
    ))]
    #[tokio::test(flavor = "multi_thread")]
    async fn opaque_predicate_prunes_files_end_to_end() {
        use std::ffi::c_void;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        use delta_kernel::arrow::array::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
        use delta_kernel::arrow::array::{
            Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, StructArray,
        };
        use delta_kernel::object_store::memory::InMemory;
        use delta_kernel::scan::state::ScanFile;
        use delta_kernel::Snapshot;
        use delta_kernel_default_engine::DefaultEngineBuilder;
        use test_utils::add_commit;

        use crate::engine_data::ArrowFFIData;
        use crate::error::EngineExecResult;

        static CALLS: AtomicUsize = AtomicUsize::new(0);

        // StatsMode `IN_RANGE`: keep a file iff the target (arg1) falls within the column's
        // [min, max] (arg0 slots 0 and 1); null bounds keep the file.
        unsafe extern "C" fn engine_in_range(
            _state: *mut c_void,
            _op_name: KernelStringSlice,
            args_in: ArrowFFIData,
            _inverted: bool,
            out: *mut EngineExecResult<ArrowFFIData>,
        ) {
            CALLS.fetch_add(1, Ordering::SeqCst);

            let ArrowFFIData { array, schema } = args_in;
            let data = unsafe { from_ffi(array, &schema) }.unwrap();
            let batch: RecordBatch = StructArray::from(data).into();

            let stats = batch
                .column(0)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            let min = stats
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let max = stats
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let target = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();

            let keep: BooleanArray = (0..batch.num_rows())
                .map(|i| {
                    if min.is_null(i) || max.is_null(i) {
                        return Some(true);
                    }
                    let t = target.value(i);
                    Some(min.value(i) <= t && t <= max.value(i))
                })
                .collect();

            let arr: ArrayRef = Arc::new(keep);
            let array_data = arr.to_data();
            let ffi = ArrowFFIData {
                array: FFI_ArrowArray::new(&array_data),
                schema: FFI_ArrowSchema::try_from(array_data.data_type()).unwrap(),
            };
            unsafe { *out = EngineExecResult::Success(ffi) };
        }
        unsafe extern "C" fn noop_free(_: *mut c_void) {}

        // Build the predicate through the FFI symbol, then extract the kernel `Predicate`.
        let mut state = KernelExpressionVisitorState::default();
        let col_id = wrap_expression(&mut state, Expression::column(["id"]));
        let target = wrap_expression(&mut state, Expression::literal(25i64));
        let (_keep, mut it) = make_iter(vec![col_id, target]);
        let name = "IN_RANGE";
        let id = ok_or_panic(unsafe {
            visit_predicate_opaque_with_eval(
                &mut state,
                kernel_string_slice!(name),
                &mut it,
                COpaqueEvalCallbacks {
                    engine_state: std::ptr::null_mut(),
                    eval_pred_rows: engine_in_range,
                    eval_pred_stats: engine_in_range,
                    free_state: noop_free,
                },
                allocate_err,
            )
        });
        let predicate = Arc::new(unwrap_kernel_predicate(&mut state, id).unwrap());

        // 3-file table: A=[0,10], B=[20,30], C=[100,110]. Only B covers 25.
        let table_root = "memory:///";
        let storage = Arc::new(InMemory::new());
        let schema = r#"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}"#;
        let add = |path: &str, lo: i64, hi: i64| {
            format!(
                r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":262,"modificationTime":1587968586000,"dataChange":true,"stats":"{{\"numRecords\":10,\"nullCount\":{{\"id\":0}},\"minValues\":{{\"id\":{lo}}},\"maxValues\":{{\"id\":{hi}}}}}"}}}}"#
            )
        };
        let commit = [
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#.to_string(),
            format!(
                r#"{{"metaData":{{"id":"t","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{schema}","partitionColumns":[],"configuration":{{}},"createdTime":1587968586000}}}}"#
            ),
            add("a.parquet", 0, 10),
            add("b.parquet", 20, 30),
            add("c.parquet", 100, 110),
        ]
        .join("\n");
        add_commit(table_root, storage.as_ref(), 0, commit)
            .await
            .unwrap();

        let engine = DefaultEngineBuilder::new(storage).build();
        let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();
        let scan = snapshot
            .scan_builder()
            .with_predicate(predicate)
            .build()
            .unwrap();

        fn push_path(paths: &mut Vec<String>, scan_file: ScanFile) {
            paths.push(scan_file.path);
        }
        let mut paths: Vec<String> = Vec::new();
        for sm in scan.scan_metadata(&engine).unwrap() {
            paths = sm.unwrap().visit_scan_files(paths, push_path).unwrap();
        }

        assert_eq!(
            paths,
            vec!["b.parquet".to_string()],
            "only [20,30] covers 25"
        );
        assert!(
            CALLS.load(Ordering::SeqCst) >= 1,
            "engine callback must fire in StatsMode"
        );
    }

    /// A misbehaving engine that violates the keep-on-null-bounds contract (returns `false` for
    /// rows with null stats, as checkpoint Remove rows present) must not corrupt Add/Remove
    /// reconciliation: kernel's `OR(NOT is_add, ...)` guard keeps Remove rows regardless of the
    /// verdict, so the tombstone still suppresses the earlier Add instead of resurrecting it.
    #[cfg(any(
        feature = "default-engine-rustls",
        feature = "default-engine-native-tls"
    ))]
    #[tokio::test(flavor = "multi_thread")]
    async fn misbehaving_stats_callback_cannot_resurrect_removed_file() {
        use std::ffi::c_void;
        use std::sync::Arc;

        use delta_kernel::arrow::array::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
        use delta_kernel::arrow::array::{Array, ArrayRef, BooleanArray, RecordBatch, StructArray};
        use delta_kernel::object_store::memory::InMemory;
        use delta_kernel::scan::state::ScanFile;
        use delta_kernel::Snapshot;
        use delta_kernel_default_engine::DefaultEngineBuilder;
        use test_utils::add_commit;

        use crate::engine_data::ArrowFFIData;
        use crate::error::EngineExecResult;

        // Contract violation: skip (false) any row whose min/max bounds are null instead of
        // keeping it. Remove rows carry null stats, so without a kernel-side guard this would
        // drop tombstones from log replay.
        unsafe extern "C" fn engine_skip_null_bounds(
            _state: *mut c_void,
            _op_name: KernelStringSlice,
            args_in: ArrowFFIData,
            _inverted: bool,
            out: *mut EngineExecResult<ArrowFFIData>,
        ) {
            let ArrowFFIData { array, schema } = args_in;
            let data = unsafe { from_ffi(array, &schema) }.unwrap();
            let batch: RecordBatch = StructArray::from(data).into();

            let stats = batch
                .column(0)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            let (min, max) = (stats.column(0), stats.column(1));
            let keep: BooleanArray = (0..batch.num_rows())
                .map(|i| Some(!min.is_null(i) && !max.is_null(i)))
                .collect();

            let arr: ArrayRef = Arc::new(keep);
            let array_data = arr.to_data();
            let ffi = ArrowFFIData {
                array: FFI_ArrowArray::new(&array_data),
                schema: FFI_ArrowSchema::try_from(array_data.data_type()).unwrap(),
            };
            unsafe { *out = EngineExecResult::Success(ffi) };
        }
        unsafe extern "C" fn noop_free(_: *mut c_void) {}

        let mut state = KernelExpressionVisitorState::default();
        let col_id = wrap_expression(&mut state, Expression::column(["id"]));
        let target = wrap_expression(&mut state, Expression::literal(25i64));
        let (_keep, mut it) = make_iter(vec![col_id, target]);
        let name = "IN_RANGE";
        let id = ok_or_panic(unsafe {
            visit_predicate_opaque_with_eval(
                &mut state,
                kernel_string_slice!(name),
                &mut it,
                COpaqueEvalCallbacks {
                    engine_state: std::ptr::null_mut(),
                    eval_pred_rows: engine_skip_null_bounds,
                    eval_pred_stats: engine_skip_null_bounds,
                    free_state: noop_free,
                },
                allocate_err,
            )
        });
        let predicate = Arc::new(unwrap_kernel_predicate(&mut state, id).unwrap());

        // Version 0 adds two files (both with stats the engine keeps); version 1 removes
        // b.parquet. Log replay must surface only a.parquet.
        let table_root = "memory:///";
        let storage = Arc::new(InMemory::new());
        let schema = r#"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}"#;
        let add = |path: &str, lo: i64, hi: i64| {
            format!(
                r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":262,"modificationTime":1587968586000,"dataChange":true,"stats":"{{\"numRecords\":10,\"nullCount\":{{\"id\":0}},\"minValues\":{{\"id\":{lo}}},\"maxValues\":{{\"id\":{hi}}}}}"}}}}"#
            )
        };
        let commit0 = [
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#.to_string(),
            format!(
                r#"{{"metaData":{{"id":"t","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{schema}","partitionColumns":[],"configuration":{{}},"createdTime":1587968586000}}}}"#
            ),
            add("a.parquet", 20, 30),
            add("b.parquet", 20, 30),
        ]
        .join("\n");
        add_commit(table_root, storage.as_ref(), 0, commit0)
            .await
            .unwrap();
        let commit1 = r#"{"remove":{"path":"b.parquet","deletionTimestamp":1587968586001,"dataChange":true}}"#.to_string();
        add_commit(table_root, storage.as_ref(), 1, commit1)
            .await
            .unwrap();

        let engine = DefaultEngineBuilder::new(storage).build();
        let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();
        let scan = snapshot
            .scan_builder()
            .with_predicate(predicate)
            .build()
            .unwrap();

        fn push_path(paths: &mut Vec<String>, scan_file: ScanFile) {
            paths.push(scan_file.path);
        }
        let mut paths: Vec<String> = Vec::new();
        for sm in scan.scan_metadata(&engine).unwrap() {
            paths = sm.unwrap().visit_scan_files(paths, push_path).unwrap();
        }

        assert_eq!(
            paths,
            vec!["a.parquet".to_string()],
            "the Remove tombstone must survive a skip-on-null-bounds verdict; \
             b.parquet reappearing means a resurrected deleted file"
        );
    }
}
