//! Child accessor for opaque-predicate arguments.
//!
//! The engine receives a [`ChildAccessor`] during pruning callbacks and uses
//! [`child_accessor_kind`] to dispatch on shape, then the typed
//! `child_accessor_*` getters to read the value.

use delta_kernel::expressions::{Expression, Scalar};

use crate::{kernel_string_slice, KernelStringSlice};

/// Tag indicating the shape of an opaque op's child argument.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpaqueChildKind {
    /// Argument shape not supported: nested expressions, multi-segment
    /// column refs, or scalar types not enumerated here.
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

    fn child(&self, idx: usize) -> Option<&Expression> {
        self.children.get(idx)
    }
}

/// Map an [`Expression`] to its [`OpaqueChildKind`] tag.
fn expression_kind_of(expr: &Expression) -> OpaqueChildKind {
    match expr {
        Expression::Column(c) if c.path().len() == 1 => OpaqueChildKind::Column,
        Expression::Literal(Scalar::String(_)) => OpaqueChildKind::LiteralString,
        Expression::Literal(Scalar::Long(_) | Scalar::Integer(_))
        | Expression::Literal(Scalar::Short(_) | Scalar::Byte(_)) => OpaqueChildKind::LiteralLong,
        Expression::Literal(Scalar::Double(_) | Scalar::Float(_)) => OpaqueChildKind::LiteralDouble,
        Expression::Literal(Scalar::Boolean(_)) => OpaqueChildKind::LiteralBool,
        Expression::Literal(Scalar::Null(_)) => OpaqueChildKind::LiteralNull,
        _ => OpaqueChildKind::Unsupported,
    }
}

/// Number of children carried by the opaque op.
///
/// # Safety
/// `acc` must be a valid pointer to a [`ChildAccessor`] valid for the
/// current callback.
#[no_mangle]
pub unsafe extern "C" fn child_accessor_count(acc: *const ChildAccessor<'_>) -> usize {
    if acc.is_null() {
        return 0;
    }
    unsafe { (&*acc).children.len() }
}

/// Returns the kind of the child at `idx`. Returns
/// [`OpaqueChildKind::Unsupported`] if `idx` is out of range or the shape
/// isn't recognized.
///
/// # Safety
/// `acc` must be a valid pointer to a [`ChildAccessor`].
#[no_mangle]
pub unsafe extern "C" fn child_accessor_kind(acc: *const ChildAccessor<'_>, idx: usize) -> u32 {
    if acc.is_null() {
        return OpaqueChildKind::Unsupported as u32;
    }
    let Some(child) = (unsafe { &*acc }).child(idx) else {
        return OpaqueChildKind::Unsupported as u32;
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
    if acc.is_null() || out.is_null() {
        return false;
    }
    let acc = unsafe { &*acc };
    let Some(Expression::Column(c)) = acc.child(idx) else {
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
    if acc.is_null() || out.is_null() {
        return false;
    }
    let acc = unsafe { &*acc };
    let Some(Expression::Literal(Scalar::String(s))) = acc.child(idx) else {
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
    if acc.is_null() || out.is_null() {
        return false;
    }
    let acc = unsafe { &*acc };
    let value = match acc.child(idx) {
        Some(Expression::Literal(Scalar::Long(v))) => *v,
        Some(Expression::Literal(Scalar::Integer(v))) => i64::from(*v),
        Some(Expression::Literal(Scalar::Short(v))) => i64::from(*v),
        Some(Expression::Literal(Scalar::Byte(v))) => i64::from(*v),
        _ => return false,
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
    if acc.is_null() || out.is_null() {
        return false;
    }
    let acc = unsafe { &*acc };
    let value = match acc.child(idx) {
        Some(Expression::Literal(Scalar::Double(v))) => *v,
        Some(Expression::Literal(Scalar::Float(v))) => f64::from(*v),
        _ => return false,
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
    if acc.is_null() || out.is_null() {
        return false;
    }
    let acc = unsafe { &*acc };
    let Some(Expression::Literal(Scalar::Boolean(v))) = acc.child(idx) else {
        return false;
    };
    unsafe { *out = *v };
    true
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::panic)]

    use std::ptr;

    use delta_kernel::expressions::{column_expr, Expression};

    use super::*;
    use crate::TryFromStringSlice;

    #[test]
    fn child_accessor_reads_column_and_literal() {
        let exprs = [column_expr!("name"), Expression::literal("foo")];
        let acc = ChildAccessor::new(&exprs);

        assert_eq!(unsafe { child_accessor_count(&acc) }, 2);
        assert_eq!(
            unsafe { child_accessor_kind(&acc, 0) },
            OpaqueChildKind::Column as u32
        );
        assert_eq!(
            unsafe { child_accessor_kind(&acc, 1) },
            OpaqueChildKind::LiteralString as u32
        );

        let mut col_out = KernelStringSlice {
            ptr: ptr::null(),
            len: 0,
        };
        assert!(unsafe { child_accessor_column_name(&acc, 0, &mut col_out) });
        assert_eq!(unsafe { String::try_from_slice(&col_out) }.unwrap(), "name");

        let mut lit_out = KernelStringSlice {
            ptr: ptr::null(),
            len: 0,
        };
        assert!(unsafe { child_accessor_literal_string(&acc, 1, &mut lit_out) });
        assert_eq!(unsafe { String::try_from_slice(&lit_out) }.unwrap(), "foo");
    }

    #[test]
    fn child_accessor_rejects_wrong_kind() {
        let exprs = [column_expr!("name"), Expression::literal("foo")];
        let acc = ChildAccessor::new(&exprs);

        let mut out = KernelStringSlice {
            ptr: ptr::null(),
            len: 0,
        };
        assert!(!unsafe { child_accessor_column_name(&acc, 1, &mut out) });
        assert!(!unsafe { child_accessor_literal_string(&acc, 0, &mut out) });
        assert_eq!(
            unsafe { child_accessor_kind(&acc, 99) },
            OpaqueChildKind::Unsupported as u32
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
        assert!((double_out - 2.5).abs() < 1e-9);

        let mut bool_out = false;
        assert!(unsafe { child_accessor_literal_bool(&acc, 2, &mut bool_out) });
        assert!(bool_out);
    }
}
