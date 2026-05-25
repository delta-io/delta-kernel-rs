//! Recursive [`OpaqueExpressionView`] accessor for opaque-predicate sub-trees.
//!
//! Engines that need to introspect compound expressions (`Unary`, `Binary`,
//! `Opaque`, `Unknown`) fetch a sub-tree pointer via
//! [`child_accessor_subtree`] and walk it with [`expression_kind`] and the
//! shape-specific `expression_*` getters.

use delta_kernel::expressions::{BinaryExpressionOp, Expression, Scalar, UnaryExpressionOp};

use super::child::{
    expression_kind_of, literal_as_f64, literal_as_i64, ChildAccessor, ExpressionKind,
};
use crate::{kernel_string_slice, KernelStringSlice};

/// Opaque pointer to a single [`Expression`] sub-tree. Valid only for the
/// duration of the surrounding `eval_against_stats` callback; engines must
/// not retain it.
///
/// Layout note: this type is `#[repr(transparent)]` over `Expression`
/// *itself*, not over a reference. A prior shape
/// `OpaqueExpressionView<'a>(&'a Expression)` segfaulted because
/// `*const OpaqueExpressionView` then carried pointer-sized data, so the
/// FFI deref read garbage instead of the `Expression`. Keep this struct
/// transparent over the owned `Expression`.
#[repr(transparent)]
pub struct OpaqueExpressionView {
    inner: Expression,
}

impl OpaqueExpressionView {
    fn as_expr(&self) -> &Expression {
        &self.inner
    }
}

/// Cast a borrowed [`Expression`] to a `*const OpaqueExpressionView`.
fn as_view(expr: &Expression) -> *const OpaqueExpressionView {
    expr as *const Expression as *const OpaqueExpressionView
}

/// Borrow the wrapped [`Expression`]. Returns `None` for a null pointer.
///
/// # Safety
/// `view` must either be null or point to a valid [`OpaqueExpressionView`]
/// for the surrounding callback's lifetime.
unsafe fn view_expr<'a>(view: *const OpaqueExpressionView) -> Option<&'a Expression> {
    unsafe { view.as_ref() }.map(OpaqueExpressionView::as_expr)
}

/// Unary expression op tag. `Unsupported = 0` is the catch-all returned for
/// null inputs, shape mismatches, or new kernel-side ops the engine hasn't
/// been recompiled for -- engines treat it as opt-out, not a stable signal.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FfiUnaryExpressionOp {
    Unsupported = 0,
    ToJson = 1,
}

/// Binary expression op tag. See [`FfiUnaryExpressionOp`] for `Unsupported`
/// semantics.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FfiBinaryExpressionOp {
    Unsupported = 0,
    Plus = 1,
    Minus = 2,
    Multiply = 3,
    Divide = 4,
}

/// Hand back an [`OpaqueExpressionView`] for child `idx`. Returns null if
/// the index is out of range or the accessor is null.
///
/// # Safety
/// `acc` must be a valid [`ChildAccessor`] pointer if non-null. The
/// returned pointer is valid for the surrounding callback's lifetime.
#[no_mangle]
pub unsafe extern "C" fn child_accessor_subtree(
    acc: *const ChildAccessor<'_>,
    idx: usize,
) -> *const OpaqueExpressionView {
    let Some(acc) = (unsafe { acc.as_ref() }) else {
        return std::ptr::null();
    };
    acc.child(idx).map_or(std::ptr::null(), as_view)
}

/// Kind of the [`Expression`] referenced by `view`. Returns
/// [`ExpressionKind::Unsupported`] for null input.
///
/// # Safety
/// `view` must be a valid [`OpaqueExpressionView`] pointer if non-null.
#[no_mangle]
pub unsafe extern "C" fn expression_kind(view: *const OpaqueExpressionView) -> u32 {
    let Some(expr) = (unsafe { view_expr(view) }) else {
        return ExpressionKind::Unsupported as u32;
    };
    expression_kind_of(expr) as u32
}

/// Read a single-segment column name from `view` (if `expression_kind` is
/// `Column`).
///
/// # Safety
/// `view` valid, `out` valid pointer. The returned slice points into
/// kernel-owned data valid for the callback's lifetime.
#[no_mangle]
pub unsafe extern "C" fn expression_column_name(
    view: *const OpaqueExpressionView,
    out: *mut KernelStringSlice,
) -> bool {
    if out.is_null() {
        return false;
    }
    let Some(Expression::Column(c)) = (unsafe { view_expr(view) }) else {
        return false;
    };
    let path = c.path();
    if path.len() != 1 {
        return false;
    }
    let name = path[0].as_str();
    unsafe { *out = kernel_string_slice!(name) };
    true
}

/// Read a string literal from `view`.
///
/// # Safety
/// See [`expression_column_name`].
#[no_mangle]
pub unsafe extern "C" fn expression_literal_string(
    view: *const OpaqueExpressionView,
    out: *mut KernelStringSlice,
) -> bool {
    if out.is_null() {
        return false;
    }
    let Some(Expression::Literal(Scalar::String(s))) = (unsafe { view_expr(view) }) else {
        return false;
    };
    let view = s.as_str();
    unsafe { *out = kernel_string_slice!(view) };
    true
}

/// Read an integer literal as `i64`, widening narrower signed integer
/// scalars.
///
/// # Safety
/// See [`expression_column_name`].
#[no_mangle]
pub unsafe extern "C" fn expression_literal_long(
    view: *const OpaqueExpressionView,
    out: *mut i64,
) -> bool {
    if out.is_null() {
        return false;
    }
    let Some(value) = (unsafe { view_expr(view) }).and_then(literal_as_i64) else {
        return false;
    };
    unsafe { *out = value };
    true
}

/// Read a floating-point literal as `f64`.
///
/// # Safety
/// See [`expression_column_name`].
#[no_mangle]
pub unsafe extern "C" fn expression_literal_double(
    view: *const OpaqueExpressionView,
    out: *mut f64,
) -> bool {
    if out.is_null() {
        return false;
    }
    let Some(value) = (unsafe { view_expr(view) }).and_then(literal_as_f64) else {
        return false;
    };
    unsafe { *out = value };
    true
}

/// Read a boolean literal.
///
/// # Safety
/// See [`expression_column_name`].
#[no_mangle]
pub unsafe extern "C" fn expression_literal_bool(
    view: *const OpaqueExpressionView,
    out: *mut bool,
) -> bool {
    if out.is_null() {
        return false;
    }
    let Some(Expression::Literal(Scalar::Boolean(v))) = (unsafe { view_expr(view) }) else {
        return false;
    };
    unsafe { *out = *v };
    true
}

/// Unary op tag for an `Expression::Unary`. Returns `Unsupported` for
/// non-unary expressions or unrecognized ops.
///
/// # Safety
/// `view` valid pointer.
#[no_mangle]
pub unsafe extern "C" fn expression_unary_op(view: *const OpaqueExpressionView) -> u32 {
    let Some(Expression::Unary(u)) = (unsafe { view_expr(view) }) else {
        return FfiUnaryExpressionOp::Unsupported as u32;
    };
    let op = match u.op {
        UnaryExpressionOp::ToJson => FfiUnaryExpressionOp::ToJson,
    };
    op as u32
}

/// Sub-tree pointer for the child of an `Expression::Unary`. Returns null
/// if `view` isn't unary.
///
/// # Safety
/// `view` valid pointer.
#[no_mangle]
pub unsafe extern "C" fn expression_unary_child(
    view: *const OpaqueExpressionView,
) -> *const OpaqueExpressionView {
    let Some(Expression::Unary(u)) = (unsafe { view_expr(view) }) else {
        return std::ptr::null();
    };
    as_view(&u.expr)
}

/// Binary op tag for an `Expression::Binary`. Returns `Unsupported` for
/// non-binary expressions.
///
/// # Safety
/// `view` valid pointer.
#[no_mangle]
pub unsafe extern "C" fn expression_binary_op(view: *const OpaqueExpressionView) -> u32 {
    let Some(Expression::Binary(b)) = (unsafe { view_expr(view) }) else {
        return FfiBinaryExpressionOp::Unsupported as u32;
    };
    let op = match b.op {
        BinaryExpressionOp::Plus => FfiBinaryExpressionOp::Plus,
        BinaryExpressionOp::Minus => FfiBinaryExpressionOp::Minus,
        BinaryExpressionOp::Multiply => FfiBinaryExpressionOp::Multiply,
        BinaryExpressionOp::Divide => FfiBinaryExpressionOp::Divide,
    };
    op as u32
}

/// Left sub-tree of an `Expression::Binary`.
///
/// # Safety
/// `view` valid pointer.
#[no_mangle]
pub unsafe extern "C" fn expression_binary_left(
    view: *const OpaqueExpressionView,
) -> *const OpaqueExpressionView {
    let Some(Expression::Binary(b)) = (unsafe { view_expr(view) }) else {
        return std::ptr::null();
    };
    as_view(&b.left)
}

/// Right sub-tree of an `Expression::Binary`.
///
/// # Safety
/// `view` valid pointer.
#[no_mangle]
pub unsafe extern "C" fn expression_binary_right(
    view: *const OpaqueExpressionView,
) -> *const OpaqueExpressionView {
    let Some(Expression::Binary(b)) = (unsafe { view_expr(view) }) else {
        return std::ptr::null();
    };
    as_view(&b.right)
}

/// Read the op name of an `Expression::Opaque` (engine-defined function).
///
/// # Safety
/// `view` valid, `out` valid pointer.
#[no_mangle]
pub unsafe extern "C" fn expression_opaque_name(
    view: *const OpaqueExpressionView,
    out: *mut KernelStringSlice,
) -> bool {
    if out.is_null() {
        return false;
    }
    let Some(Expression::Opaque(o)) = (unsafe { view_expr(view) }) else {
        return false;
    };
    let name = o.op.name();
    unsafe { *out = kernel_string_slice!(name) };
    true
}

/// Number of args carried by an `Expression::Opaque`.
///
/// # Safety
/// `view` valid pointer.
#[no_mangle]
pub unsafe extern "C" fn expression_opaque_arg_count(view: *const OpaqueExpressionView) -> usize {
    let Some(Expression::Opaque(o)) = (unsafe { view_expr(view) }) else {
        return 0;
    };
    o.exprs.len()
}

/// Fetch arg `idx` of an `Expression::Opaque` as an [`OpaqueExpressionView`].
/// Returns null if out of range or the expression isn't opaque.
///
/// # Safety
/// `view` valid pointer.
#[no_mangle]
pub unsafe extern "C" fn expression_opaque_arg(
    view: *const OpaqueExpressionView,
    idx: usize,
) -> *const OpaqueExpressionView {
    let Some(Expression::Opaque(o)) = (unsafe { view_expr(view) }) else {
        return std::ptr::null();
    };
    o.exprs.get(idx).map_or(std::ptr::null(), as_view)
}

/// Read the carrier name of an `Expression::Unknown`.
///
/// # Safety
/// `view` valid, `out` valid pointer.
#[no_mangle]
pub unsafe extern "C" fn expression_unknown_name(
    view: *const OpaqueExpressionView,
    out: *mut KernelStringSlice,
) -> bool {
    if out.is_null() {
        return false;
    }
    let Some(Expression::Unknown(name)) = (unsafe { view_expr(view) }) else {
        return false;
    };
    let view = name.as_str();
    unsafe { *out = kernel_string_slice!(view) };
    true
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::panic)]

    use std::ptr;

    use delta_kernel::expressions::{column_expr, BinaryExpressionOp, Expression, Scalar};
    use delta_kernel::schema::DataType;
    use rstest::rstest;

    use super::*;
    use crate::TryFromStringSlice;

    fn empty_slice() -> KernelStringSlice {
        KernelStringSlice {
            ptr: ptr::null(),
            len: 0,
        }
    }

    #[test]
    fn child_accessor_recurses_into_binary_expression() {
        let plus = Expression::binary(
            BinaryExpressionOp::Plus,
            column_expr!("col1"),
            column_expr!("col2"),
        );
        let exprs = [plus, Expression::literal("foo")];
        let acc = ChildAccessor::new(&exprs);

        assert_eq!(
            unsafe { super::super::child::child_accessor_kind(&acc, 0) },
            ExpressionKind::BinaryExpression as u32
        );

        let sub = unsafe { child_accessor_subtree(&acc, 0) };
        assert!(!sub.is_null());
        assert_eq!(
            unsafe { expression_kind(sub) },
            ExpressionKind::BinaryExpression as u32
        );
        assert_eq!(
            unsafe { expression_binary_op(sub) },
            FfiBinaryExpressionOp::Plus as u32
        );

        let left = unsafe { expression_binary_left(sub) };
        assert!(!left.is_null());
        assert_eq!(
            unsafe { expression_kind(left) },
            ExpressionKind::Column as u32
        );
        let mut name = empty_slice();
        assert!(unsafe { expression_column_name(left, &mut name) });
        assert_eq!(unsafe { String::try_from_slice(&name) }.unwrap(), "col1");

        let right = unsafe { expression_binary_right(sub) };
        assert!(!right.is_null());
        assert!(unsafe { expression_column_name(right, &mut name) });
        assert_eq!(unsafe { String::try_from_slice(&name) }.unwrap(), "col2");
    }

    #[test]
    fn child_accessor_subtree_returns_null_for_out_of_range() {
        let exprs = [column_expr!("c")];
        let acc = ChildAccessor::new(&exprs);
        assert!(unsafe { child_accessor_subtree(&acc, 99) }.is_null());
        assert!(unsafe { child_accessor_subtree(std::ptr::null(), 0) }.is_null());
    }

    #[test]
    fn expression_unknown_carries_name() {
        let exprs = [Expression::Unknown("mystery".to_string())];
        let acc = ChildAccessor::new(&exprs);
        let sub = unsafe { child_accessor_subtree(&acc, 0) };
        assert_eq!(
            unsafe { expression_kind(sub) },
            ExpressionKind::Unknown as u32
        );
        let mut name = empty_slice();
        assert!(unsafe { expression_unknown_name(sub, &mut name) });
        assert_eq!(unsafe { String::try_from_slice(&name) }.unwrap(), "mystery");
    }

    #[derive(Debug, PartialEq)]
    struct FakeLowerOp;

    impl delta_kernel::expressions::OpaqueExpressionOp for FakeLowerOp {
        fn name(&self) -> &str {
            "LOWER"
        }
        fn eval_expr_scalar(
            &self,
            _eval_expr: &delta_kernel::expressions::ScalarExpressionEvaluator<'_>,
            _exprs: &[Expression],
        ) -> delta_kernel::DeltaResult<Scalar> {
            Ok(Scalar::Null(DataType::STRING))
        }
    }

    #[test]
    fn child_accessor_recurses_into_opaque_function_call() {
        let lower_col = Expression::opaque(FakeLowerOp, [column_expr!("col")]);
        let exprs = [lower_col, Expression::literal("abc")];
        let acc = ChildAccessor::new(&exprs);

        assert_eq!(
            unsafe { super::super::child::child_accessor_kind(&acc, 0) },
            ExpressionKind::OpaqueExpression as u32
        );

        let sub = unsafe { child_accessor_subtree(&acc, 0) };
        assert!(!sub.is_null());
        let mut name = empty_slice();
        assert!(unsafe { expression_opaque_name(sub, &mut name) });
        assert_eq!(unsafe { String::try_from_slice(&name) }.unwrap(), "LOWER");
        assert_eq!(unsafe { expression_opaque_arg_count(sub) }, 1);

        let inner = unsafe { expression_opaque_arg(sub, 0) };
        assert!(!inner.is_null());
        assert_eq!(
            unsafe { expression_kind(inner) },
            ExpressionKind::Column as u32
        );
        assert!(unsafe { expression_column_name(inner, &mut name) });
        assert_eq!(unsafe { String::try_from_slice(&name) }.unwrap(), "col");
    }

    #[test]
    fn expression_getters_reject_null_and_shape_mismatch() {
        assert_eq!(
            unsafe { expression_kind(ptr::null()) },
            ExpressionKind::Unsupported as u32
        );
        assert!(unsafe { expression_binary_left(ptr::null()).is_null() });
        assert!(unsafe { expression_opaque_arg(ptr::null(), 0).is_null() });
        let mut name = empty_slice();
        assert!(!unsafe { expression_column_name(ptr::null(), &mut name) });

        let exprs = [column_expr!("c")];
        let acc = ChildAccessor::new(&exprs);
        let sub = unsafe { child_accessor_subtree(&acc, 0) };
        assert!(unsafe { expression_binary_left(sub).is_null() });
        assert_eq!(
            unsafe { expression_binary_op(sub) },
            FfiBinaryExpressionOp::Unsupported as u32
        );
        assert!(!unsafe { expression_opaque_name(sub, &mut name) });
    }

    // == widening: every signed integer scalar widens to i64 via the same getter ==

    #[rstest]
    #[case::long(Scalar::Long(i64::MIN), i64::MIN)]
    #[case::integer(Scalar::Integer(-42), -42)]
    #[case::short(Scalar::Short(-1), -1)]
    #[case::byte(Scalar::Byte(-128), -128)]
    fn expression_literal_long_widens_signed_integer_scalars(
        #[case] scalar: Scalar,
        #[case] expected: i64,
    ) {
        let exprs = [Expression::Literal(scalar)];
        let acc = ChildAccessor::new(&exprs);
        let sub = unsafe { child_accessor_subtree(&acc, 0) };
        let mut out = 0_i64;
        assert!(unsafe { expression_literal_long(sub, &mut out) });
        assert_eq!(out, expected);
    }

    // == binary op tag: every BinaryExpressionOp variant maps to a distinct FFI tag ==

    #[rstest]
    #[case(BinaryExpressionOp::Plus, FfiBinaryExpressionOp::Plus)]
    #[case(BinaryExpressionOp::Minus, FfiBinaryExpressionOp::Minus)]
    #[case(BinaryExpressionOp::Multiply, FfiBinaryExpressionOp::Multiply)]
    #[case(BinaryExpressionOp::Divide, FfiBinaryExpressionOp::Divide)]
    fn expression_binary_op_round_trips_every_variant(
        #[case] kernel_op: BinaryExpressionOp,
        #[case] ffi_op: FfiBinaryExpressionOp,
    ) {
        let expr = Expression::binary(kernel_op, column_expr!("a"), column_expr!("b"));
        let exprs = [expr];
        let acc = ChildAccessor::new(&exprs);
        let sub = unsafe { child_accessor_subtree(&acc, 0) };
        assert_eq!(unsafe { expression_binary_op(sub) }, ffi_op as u32);
    }

    // == unary positive path: ToJson maps and the child sub-tree resolves ==

    #[test]
    fn expression_unary_to_json_round_trips_with_child() {
        let inner = column_expr!("payload");
        let unary = Expression::unary(UnaryExpressionOp::ToJson, inner);
        let exprs = [unary];
        let acc = ChildAccessor::new(&exprs);
        let sub = unsafe { child_accessor_subtree(&acc, 0) };

        assert_eq!(
            unsafe { expression_kind(sub) },
            ExpressionKind::UnaryExpression as u32
        );
        assert_eq!(
            unsafe { expression_unary_op(sub) },
            FfiUnaryExpressionOp::ToJson as u32
        );

        let child = unsafe { expression_unary_child(sub) };
        assert!(!child.is_null());
        assert_eq!(
            unsafe { expression_kind(child) },
            ExpressionKind::Column as u32
        );
        let mut name = empty_slice();
        assert!(unsafe { expression_column_name(child, &mut name) });
        assert_eq!(unsafe { String::try_from_slice(&name) }.unwrap(), "payload");
    }

    // == opaque arg out-of-range: returns null without UB ==

    #[test]
    fn expression_opaque_arg_returns_null_for_out_of_range() {
        let opaque = Expression::opaque(FakeLowerOp, [column_expr!("c")]);
        let exprs = [opaque];
        let acc = ChildAccessor::new(&exprs);
        let sub = unsafe { child_accessor_subtree(&acc, 0) };
        assert_eq!(unsafe { expression_opaque_arg_count(sub) }, 1);
        assert!(unsafe { expression_opaque_arg(sub, 1) }.is_null());
        assert!(unsafe { expression_opaque_arg(sub, usize::MAX) }.is_null());
    }
}
