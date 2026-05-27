//! Child accessor for opaque-predicate arguments.
//!
//! The engine receives a [`ChildAccessor`] during pruning callbacks and uses
//! [`child_accessor_kind`] to dispatch on shape, then the typed
//! `child_accessor_*` getters to read the value.

use delta_kernel::expressions::{Expression, Scalar};

use crate::{kernel_string_slice, KernelStringSlice};

/// Tag for the shape of an [`OpaqueExpressionView`].
///
/// Leaf kinds (`Column`, `Literal*`) can be read directly with the flat
/// `child_accessor_*` getters or via the `expression_*_literal_*` /
/// `expression_column_name` family. Compound kinds (`UnaryExpression`,
/// `BinaryExpression`, `OpaqueExpression`, `Unknown`) carry a sub-tree --
/// engines fetch it via [`child_accessor_subtree`] and walk with
/// [`expression_kind`] and the shape-specific `expression_*` getters.
///
/// The enum is intentionally a closed set: variants kernel cannot map to a
/// shape engines know how to read (e.g. `Expression::Predicate`,
/// `Expression::Variadic`, struct/array/map literals) fall back to
/// [`ExpressionKind::Unsupported`]. Engines treat `Unsupported` as
/// "abstain" -- new variants can be added later without forcing a C-side
/// recompile of engines that already opt out of unknown shapes.
///
/// [`OpaqueExpressionView`]: super::expression::OpaqueExpressionView
/// [`child_accessor_subtree`]: super::expression::child_accessor_subtree
/// [`expression_kind`]: super::expression::expression_kind
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpressionKind {
    /// Shape not recognized: multi-segment column refs, scalar types not
    /// enumerated here, predicate-typed children, struct/array/map literals,
    /// or compound shapes not yet wired through.
    Unsupported = 0,
    /// Single-segment column reference; read via
    /// [`child_accessor_column_name`].
    Column = 1,
    /// String literal; read via [`child_accessor_literal_string`].
    LiteralString = 2,
    /// Integer literal widened to `i64`; read via
    /// [`child_accessor_literal_long`].
    LiteralLong = 3,
    /// Floating-point literal widened to `f64`; read via
    /// [`child_accessor_literal_double`].
    LiteralDouble = 4,
    /// Boolean literal; read via [`child_accessor_literal_bool`].
    LiteralBool = 5,
    /// SQL NULL literal of any type; no value to read.
    LiteralNull = 6,
    /// Unary expression (e.g. `to_json(...)`). Descend via
    /// [`super::expression::child_accessor_subtree`].
    UnaryExpression = 7,
    /// Binary arithmetic expression (`+`, `-`, `*`, `/`). Descend via
    /// [`super::expression::child_accessor_subtree`].
    BinaryExpression = 8,
    /// Engine-defined function call. Descend via
    /// [`super::expression::child_accessor_subtree`].
    OpaqueExpression = 9,
    /// Placeholder kernel uses for engine-defined nodes it cannot
    /// introspect. Read the carrier name via
    /// [`super::expression::expression_unknown_name`].
    Unknown = 10,
}

/// Read accessor over an opaque op's child expressions. Borrowed for the
/// duration of the callback; engines must not retain it.
pub struct ChildAccessor<'a> {
    children: &'a [Expression],
}

impl<'a> ChildAccessor<'a> {
    #[allow(dead_code)] // used under default-engine-base
    pub(crate) fn new(children: &'a [Expression]) -> Self {
        Self { children }
    }

    pub(super) fn child(&self, idx: usize) -> Option<&Expression> {
        self.children.get(idx)
    }
}

/// Borrow child `idx` from `acc`. Returns `None` for a null pointer or
/// out-of-range index.
///
/// # Safety
/// `acc` must either be null or point to a [`ChildAccessor`] valid for the
/// surrounding callback.
unsafe fn acc_child<'a>(acc: *const ChildAccessor<'a>, idx: usize) -> Option<&'a Expression> {
    unsafe { acc.as_ref() }?.child(idx)
}

/// Map an [`Expression`] to its [`ExpressionKind`] tag.
pub(super) fn expression_kind_of(expr: &Expression) -> ExpressionKind {
    match expr {
        Expression::Column(c) if c.path().len() == 1 => ExpressionKind::Column,
        Expression::Literal(Scalar::String(_)) => ExpressionKind::LiteralString,
        Expression::Literal(Scalar::Long(_) | Scalar::Integer(_))
        | Expression::Literal(Scalar::Short(_) | Scalar::Byte(_)) => ExpressionKind::LiteralLong,
        Expression::Literal(Scalar::Double(_) | Scalar::Float(_)) => ExpressionKind::LiteralDouble,
        Expression::Literal(Scalar::Boolean(_)) => ExpressionKind::LiteralBool,
        Expression::Literal(Scalar::Null(_)) => ExpressionKind::LiteralNull,
        Expression::Unary(_) => ExpressionKind::UnaryExpression,
        Expression::Binary(_) => ExpressionKind::BinaryExpression,
        Expression::Opaque(_) => ExpressionKind::OpaqueExpression,
        Expression::Unknown(_) => ExpressionKind::Unknown,
        _ => ExpressionKind::Unsupported,
    }
}

/// Number of children carried by the opaque op.
///
/// # Safety
/// `acc` must be a valid pointer to a [`ChildAccessor`] valid for the
/// current callback.
#[no_mangle]
pub unsafe extern "C" fn child_accessor_count(acc: *const ChildAccessor<'_>) -> usize {
    unsafe { acc.as_ref() }.map_or(0, |a| a.children.len())
}

/// Returns the kind of the child at `idx`. Returns
/// [`ExpressionKind::Unsupported`] if `idx` is out of range or the shape
/// isn't recognized.
///
/// # Safety
/// `acc` must be a valid pointer to a [`ChildAccessor`].
#[no_mangle]
pub unsafe extern "C" fn child_accessor_kind(acc: *const ChildAccessor<'_>, idx: usize) -> u32 {
    let Some(child) = (unsafe { acc_child(acc, idx) }) else {
        return ExpressionKind::Unsupported as u32;
    };
    expression_kind_of(child) as u32
}

/// Read the single-segment column name of child `idx`. Returns `false` if
/// the child isn't a column or `idx` is out of range.
///
/// # Safety
/// `acc` valid, `out` valid pointer.
#[no_mangle]
pub unsafe extern "C" fn child_accessor_column_name(
    acc: *const ChildAccessor<'_>,
    idx: usize,
    out: *mut KernelStringSlice,
) -> bool {
    if out.is_null() {
        return false;
    }
    let Some(Expression::Column(c)) = (unsafe { acc_child(acc, idx) }) else {
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

/// Read a string-literal child.
///
/// # Safety
/// `acc` valid, `out` valid pointer. The returned slice points into
/// kernel-owned data valid for the callback's lifetime.
#[no_mangle]
pub unsafe extern "C" fn child_accessor_literal_string(
    acc: *const ChildAccessor<'_>,
    idx: usize,
    out: *mut KernelStringSlice,
) -> bool {
    if out.is_null() {
        return false;
    }
    let Some(Expression::Literal(Scalar::String(s))) = (unsafe { acc_child(acc, idx) }) else {
        return false;
    };
    let view = s.as_str();
    unsafe { *out = kernel_string_slice!(view) };
    true
}

/// Read an integer-literal child as i64. Accepts any signed integer scalar
/// (`Long`, `Integer`, `Short`, `Byte`), widened to i64.
///
/// # Safety
/// `acc` valid, `out` valid pointer.
#[no_mangle]
pub unsafe extern "C" fn child_accessor_literal_long(
    acc: *const ChildAccessor<'_>,
    idx: usize,
    out: *mut i64,
) -> bool {
    if out.is_null() {
        return false;
    }
    let Some(value) = (unsafe { acc_child(acc, idx) }).and_then(literal_as_i64) else {
        return false;
    };
    unsafe { *out = value };
    true
}

/// Read a floating-point literal child as f64.
///
/// # Safety
/// `acc` valid, `out` valid pointer.
#[no_mangle]
pub unsafe extern "C" fn child_accessor_literal_double(
    acc: *const ChildAccessor<'_>,
    idx: usize,
    out: *mut f64,
) -> bool {
    if out.is_null() {
        return false;
    }
    let Some(value) = (unsafe { acc_child(acc, idx) }).and_then(literal_as_f64) else {
        return false;
    };
    unsafe { *out = value };
    true
}

/// Read a boolean-literal child.
///
/// # Safety
/// `acc` valid, `out` valid pointer.
#[no_mangle]
pub unsafe extern "C" fn child_accessor_literal_bool(
    acc: *const ChildAccessor<'_>,
    idx: usize,
    out: *mut bool,
) -> bool {
    if out.is_null() {
        return false;
    }
    let Some(Expression::Literal(Scalar::Boolean(v))) = (unsafe { acc_child(acc, idx) }) else {
        return false;
    };
    unsafe { *out = *v };
    true
}

/// Widen any signed integer literal to i64. Returns `None` for non-integer
/// or non-literal expressions.
pub(super) fn literal_as_i64(expr: &Expression) -> Option<i64> {
    match expr {
        Expression::Literal(Scalar::Long(v)) => Some(*v),
        Expression::Literal(Scalar::Integer(v)) => Some(i64::from(*v)),
        Expression::Literal(Scalar::Short(v)) => Some(i64::from(*v)),
        Expression::Literal(Scalar::Byte(v)) => Some(i64::from(*v)),
        _ => None,
    }
}

/// Widen a floating-point literal to f64. Returns `None` for non-float
/// or non-literal expressions.
pub(super) fn literal_as_f64(expr: &Expression) -> Option<f64> {
    match expr {
        Expression::Literal(Scalar::Double(v)) => Some(*v),
        Expression::Literal(Scalar::Float(v)) => Some(f64::from(*v)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::panic)]

    use std::ptr;

    use delta_kernel::expressions::{column_expr, Expression};

    use super::*;
    use crate::TryFromStringSlice;

    fn empty_slice() -> KernelStringSlice {
        KernelStringSlice {
            ptr: ptr::null(),
            len: 0,
        }
    }

    #[test]
    fn child_accessor_reads_column_and_literal() {
        let exprs = [column_expr!("name"), Expression::literal("foo")];
        let acc = ChildAccessor::new(&exprs);

        assert_eq!(unsafe { child_accessor_count(&acc) }, 2);
        assert_eq!(
            unsafe { child_accessor_kind(&acc, 0) },
            ExpressionKind::Column as u32
        );
        assert_eq!(
            unsafe { child_accessor_kind(&acc, 1) },
            ExpressionKind::LiteralString as u32
        );

        let mut col_out = empty_slice();
        assert!(unsafe { child_accessor_column_name(&acc, 0, &mut col_out) });
        assert_eq!(unsafe { String::try_from_slice(&col_out) }.unwrap(), "name");

        let mut lit_out = empty_slice();
        assert!(unsafe { child_accessor_literal_string(&acc, 1, &mut lit_out) });
        assert_eq!(unsafe { String::try_from_slice(&lit_out) }.unwrap(), "foo");
    }

    #[test]
    fn child_accessor_rejects_wrong_kind() {
        let exprs = [column_expr!("name"), Expression::literal("foo")];
        let acc = ChildAccessor::new(&exprs);

        let mut out = empty_slice();
        assert!(!unsafe { child_accessor_column_name(&acc, 1, &mut out) });
        assert!(!unsafe { child_accessor_literal_string(&acc, 0, &mut out) });
        assert_eq!(
            unsafe { child_accessor_kind(&acc, 99) },
            ExpressionKind::Unsupported as u32
        );
    }

    #[test]
    fn child_accessor_reads_long_and_double() {
        let exprs = [
            Expression::literal(42_i64),
            Expression::literal(2.5_f64),
            Expression::literal(true),
        ];
        let acc = ChildAccessor::new(&exprs);

        let mut long_out = 0_i64;
        assert!(unsafe { child_accessor_literal_long(&acc, 0, &mut long_out) });
        assert_eq!(long_out, 42);

        let mut double_out = 0_f64;
        assert!(unsafe { child_accessor_literal_double(&acc, 1, &mut double_out) });
        assert_eq!(double_out, 2.5);

        let mut bool_out = false;
        assert!(unsafe { child_accessor_literal_bool(&acc, 2, &mut bool_out) });
        assert!(bool_out);
    }
}
