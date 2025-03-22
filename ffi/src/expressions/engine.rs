//! Defines [`KernelExpressionVisitorState`]. This is a visitor  that can be used by an [`EnginePredicate`]
//! to convert engine expressions into kernel expressions.
use std::ffi::c_void;

use crate::{
    AllocateErrorFn, EngineIterator, ExternResult, IntoExternResult, KernelStringSlice,
    ReferenceSet, TryFromStringSlice,
};
use delta_kernel::expressions::{
    BinaryExpressionOp, BinaryPredicateOp, ColumnName, Expression, Predicate, UnaryPredicateOp,
};
use delta_kernel::DeltaResult;

pub enum ExpressionOrPredicate {
    Expression(Expression),
    Predicate(Predicate),
}

#[derive(Default)]
pub struct KernelExpressionVisitorState {
    inflight_ids: ReferenceSet<ExpressionOrPredicate>,
}

/// A predicate that can be used to skip data when scanning.
///
/// When invoking [`scan::scan`], The engine provides a pointer to the (engine's native) predicate,
/// along with a visitor function that can be invoked to recursively visit the predicate. This
/// engine state must be valid until the call to [`scan::scan`] returns. Inside that method, the
/// kernel allocates visitor state, which becomes the second argument to the predicate visitor
/// invocation along with the engine-provided predicate pointer. The visitor state is valid for the
/// lifetime of the predicate visitor invocation. Thanks to this double indirection, engine and
/// kernel each retain ownership of their respective objects, with no need to coordinate memory
/// lifetimes with the other.
///
/// [`scan::scan`]: crate::scan::scan
#[repr(C)]
pub struct EnginePredicate {
    pub predicate: *mut c_void,
    pub visitor:
        extern "C" fn(predicate: *mut c_void, state: &mut KernelExpressionVisitorState) -> usize,
}

fn wrap_expression(state: &mut KernelExpressionVisitorState, expr: impl Into<Expression>) -> usize {
    state
        .inflight_ids
        .insert(ExpressionOrPredicate::Expression(expr.into()))
}

fn wrap_predicate(state: &mut KernelExpressionVisitorState, pred: impl Into<Predicate>) -> usize {
    state
        .inflight_ids
        .insert(ExpressionOrPredicate::Predicate(pred.into()))
}

pub(crate) fn unwrap_kernel_expression(
    state: &mut KernelExpressionVisitorState,
    exprid: usize,
) -> Option<Expression> {
    match state.inflight_ids.take(exprid)? {
        ExpressionOrPredicate::Expression(expr) => Some(expr),
        ExpressionOrPredicate::Predicate(pred) => Some(Expression::predicate(pred)),
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
    let left = unwrap_kernel_expression(state, a);
    let right = unwrap_kernel_expression(state, b);
    match left.zip(right) {
        Some((left, right)) => wrap_expression(state, Expression::binary(op, left, right)),
        None => 0, // invalid child => invalid node
    }
}

fn visit_predicate_binary(
    state: &mut KernelExpressionVisitorState,
    op: BinaryPredicateOp,
    a: usize,
    b: usize,
) -> usize {
    let left = unwrap_kernel_expression(state, a);
    let right = unwrap_kernel_expression(state, b);
    match left.zip(right) {
        Some((left, right)) => wrap_predicate(state, Predicate::binary(op, left, right)),
        None => 0, // invalid child => invalid node
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
    visit_predicate_binary(state, BinaryPredicateOp::LessThanOrEqual, a, b)
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
    visit_predicate_binary(state, BinaryPredicateOp::GreaterThanOrEqual, a, b)
}

#[no_mangle]
pub extern "C" fn visit_predicate_eq(
    state: &mut KernelExpressionVisitorState,
    a: usize,
    b: usize,
) -> usize {
    visit_predicate_binary(state, BinaryPredicateOp::Equal, a, b)
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
