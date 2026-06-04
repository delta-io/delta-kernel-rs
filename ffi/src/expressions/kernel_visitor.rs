//! Defines [`KernelExpressionVisitorState`]. This is a visitor that can be used to convert an
//! engine's native expressions into kernel's [`Expression`] and [`Predicate`] types.
use std::sync::Arc;

use delta_kernel::expressions::{
    BinaryExpressionOp, BinaryPredicateOp, ColumnName, Expression, Predicate, Scalar,
    UnaryPredicateOp,
};
use delta_kernel::schema::{DataType, PrimitiveType};
use delta_kernel::DeltaResult;

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
                PrimitiveType::Decimal(dt) => (Self::Decimal, dt.precision(), dt.scale()),
                // Void has no dedicated FFI tag. The current predicate-construction path is
                // not expected to produce a void-typed literal null; if one ever reaches this
                // code we want it represented as a non-primitive null rather than a missing
                // case, so this arm is a defensive fallback rather than part of the normal
                // void-column path.
                PrimitiveType::Void => (Self::NonPrimitive, 0, 0),
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
    use delta_kernel::expressions::JunctionPredicateOp;
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
        let struct_type = DataType::Struct(Box::new(
            StructType::try_new(vec![StructField::not_null("a", DataType::INTEGER)]).unwrap(),
        ));
        assert_eq!(
            NullTypeTag::from_data_type(&struct_type),
            (NullTypeTag::NonPrimitive, 0, 0)
        );

        let array_type = DataType::Array(Box::new(ArrayType::new(DataType::INTEGER, false)));
        assert_eq!(
            NullTypeTag::from_data_type(&array_type),
            (NullTypeTag::NonPrimitive, 0, 0)
        );

        let map_type = DataType::Map(Box::new(MapType::new(
            DataType::STRING,
            DataType::INTEGER,
            false,
        )));
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
}
