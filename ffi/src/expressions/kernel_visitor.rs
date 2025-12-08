//! Defines [`KernelExpressionVisitorState`]. This is a visitor that can be used to convert an
//! engine's native expressions into kernel's [`Expression`] and [`Predicate`] types.
use crate::{
    AllocateErrorFn, EngineIterator, ExternResult, IntoExternResult, KernelStringSlice,
    ReferenceSet, TryFromStringSlice,
};
use delta_kernel::expressions::{
    BinaryExpressionOp, BinaryPredicateOp, ColumnName, Expression, Predicate, Scalar,
    UnaryPredicateOp,
};
use delta_kernel::DeltaResult;

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

/// visit a timestamp_ntz literal expression 'value' (i64 representing microseconds since unix epoch)
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

/// Visit a null literal expression.
///
/// Returns an error because NULL literal reconstruction is not supported - type information
/// is lost when converting from kernel to engine format, so we cannot faithfully reconstruct
/// the original NULL literal.
#[no_mangle]
pub extern "C" fn visit_expression_literal_null(
    _state: &mut KernelExpressionVisitorState,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    let err = delta_kernel::Error::generic("NULL literal reconstruction is not supported");
    // SAFETY: The allocate_error function pointer is provided by the engine and assumed valid.
    unsafe { Err(err).into_extern_result(&allocate_error) }
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

use crate::expressions::{SharedExpression, SharedPredicate};
use crate::handle::Handle;
use crate::scan::{EngineExpression, EnginePredicate};
use std::sync::Arc;

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
            "Invalid expression ID {} returned from engine visitor",
            expr_id
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
            "Invalid predicate ID {} returned from engine visitor",
            pred_id
        ))
    })?;

    Ok(Arc::new(pred).into())
}
