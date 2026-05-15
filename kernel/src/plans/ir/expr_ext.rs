//! Fluent extension traits and free functions for [`Expression`] / [`Predicate`] construction.
//!
//! Plan builders today drown in `Expression::column(["a", "b"])` /
//! `Predicate::or_from([Predicate::is_not_null(...), ...])` chains. This module adds a thin
//! veneer that lets the same logic read as method chains:
//!
//! ```ignore
//! use delta_kernel::plans::ir::expr_ext::{any_of, col, lit, PredicateExt};
//!
//! let p = col(("add", "path")).is_null().or(col("size").gt(lit(0i64)));
//! let any = any_of([col("a").is_null(), col("b").is_not_null()]);
//! ```
//!
//! No new wrapper types — the traits attach methods to the existing [`Expression`] /
//! [`Predicate`] enums.

use crate::expressions::{ColumnName, Expression, Predicate, Scalar, UnaryExpressionOp};
use crate::schema::DataType;

// === Free functions ===

/// Column reference. Accepts:
/// - `col("name")` — single segment
/// - `col(["add", "path"])` — array of segments
/// - `col(("add", "path"))` — tuple of segments (up to 4)
/// - `col(ColumnName::new([...]))` — pre-built path
pub fn col<N: IntoColumnName>(name: N) -> Expression {
    Expression::Column(name.into_column_name())
}

/// Literal expression. Any value convertible to [`Scalar`] works.
pub fn lit<S: Into<Scalar>>(value: S) -> Expression {
    Expression::literal(value)
}

/// Typed NULL literal.
pub fn null_of(data_type: DataType) -> Expression {
    Expression::literal(Scalar::Null(data_type))
}

/// Logical AND of all predicates. Empty input normalizes to `true`.
/// See [`Predicate::and_from`].
pub fn all_of(preds: impl IntoIterator<Item = Predicate>) -> Predicate {
    Predicate::and_from(preds)
}

/// Logical OR of all predicates. Empty input normalizes to `false`.
/// See [`Predicate::or_from`].
pub fn any_of(preds: impl IntoIterator<Item = Predicate>) -> Predicate {
    Predicate::or_from(preds)
}

// === Column-name ergonomics ===

/// Conversion into a [`ColumnName`] from common shapes (single string, array, tuple).
pub trait IntoColumnName {
    fn into_column_name(self) -> ColumnName;
}

impl IntoColumnName for ColumnName {
    fn into_column_name(self) -> ColumnName {
        self
    }
}

impl IntoColumnName for &str {
    fn into_column_name(self) -> ColumnName {
        ColumnName::new([self])
    }
}

impl IntoColumnName for String {
    fn into_column_name(self) -> ColumnName {
        ColumnName::new([self])
    }
}

impl<A: Into<String>, const N: usize> IntoColumnName for [A; N] {
    fn into_column_name(self) -> ColumnName {
        ColumnName::new(self.into_iter().map(Into::into))
    }
}

impl<A: Into<String>, B: Into<String>> IntoColumnName for (A, B) {
    fn into_column_name(self) -> ColumnName {
        ColumnName::new([self.0.into(), self.1.into()])
    }
}

impl<A: Into<String>, B: Into<String>, C: Into<String>> IntoColumnName for (A, B, C) {
    fn into_column_name(self) -> ColumnName {
        ColumnName::new([self.0.into(), self.1.into(), self.2.into()])
    }
}

impl<A: Into<String>, B: Into<String>, C: Into<String>, D: Into<String>> IntoColumnName
    for (A, B, C, D)
{
    fn into_column_name(self) -> ColumnName {
        ColumnName::new([self.0.into(), self.1.into(), self.2.into(), self.3.into()])
    }
}

// === ExpressionExt ===

/// Method-style helpers that augment [`Expression`]'s inherent API.
pub trait ExpressionExt: Sized {
    /// `COALESCE(self, lit(default))` — substitute NULL with a default scalar.
    fn or_lit<S: Into<Scalar>>(self, default: S) -> Expression;

    /// `COALESCE(self, other)` — substitute NULL with another expression.
    fn or_else(self, other: impl Into<Expression>) -> Expression;

    /// `TO_JSON(self)` — encode this expression as a JSON-formatted string column.
    fn to_json(self) -> Expression;
}

impl ExpressionExt for Expression {
    fn or_lit<S: Into<Scalar>>(self, default: S) -> Expression {
        Expression::coalesce([self, Expression::literal(default)])
    }
    fn or_else(self, other: impl Into<Expression>) -> Expression {
        Expression::coalesce([self, other.into()])
    }
    fn to_json(self) -> Expression {
        Expression::unary(UnaryExpressionOp::ToJson, self)
    }
}

// === PredicateExt ===

/// Method-style boolean combinators on [`Predicate`].
///
/// `Predicate::and` / `::or` / `::not` exist but are associated functions — calling them as
/// methods (`pred.and(other)`) requires this trait.
pub trait PredicateExt: Sized {
    fn and(self, other: impl Into<Predicate>) -> Predicate;
    fn or(self, other: impl Into<Predicate>) -> Predicate;
    fn not(self) -> Predicate;
}

impl PredicateExt for Predicate {
    fn and(self, other: impl Into<Predicate>) -> Predicate {
        Predicate::and(self, other)
    }
    fn or(self, other: impl Into<Predicate>) -> Predicate {
        Predicate::or(self, other)
    }
    fn not(self) -> Predicate {
        Predicate::not(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::ColumnName;

    #[test]
    fn col_accepts_str_array_tuple_and_columnname() {
        assert_eq!(col("name"), Expression::Column(ColumnName::new(["name"])),);
        assert_eq!(
            col(["add", "path"]),
            Expression::Column(ColumnName::new(["add", "path"])),
        );
        assert_eq!(
            col(("add", "path")),
            Expression::Column(ColumnName::new(["add", "path"])),
        );
        assert_eq!(
            col(ColumnName::new(["x"])),
            Expression::Column(ColumnName::new(["x"])),
        );
    }

    #[test]
    fn predicate_ext_chains_logical_combinators() {
        let p1 = col("x").is_null();
        let p2 = col("y").is_not_null();
        let p3 = col("z").gt(lit(0i64));

        let combined = p1.clone().or(p2.clone()).and(p3.clone());
        let expected = Predicate::and(Predicate::or(p1, p2), p3);
        assert_eq!(combined, expected);
    }

    #[test]
    fn any_of_and_all_of_match_or_and_from() {
        let preds = [
            col("a").is_null(),
            col("b").is_not_null(),
            col("c").eq(lit(1i64)),
        ];
        assert_eq!(any_of(preds.clone()), Predicate::or_from(preds.clone()));
        assert_eq!(all_of(preds.clone()), Predicate::and_from(preds));
    }

    #[test]
    fn or_lit_uses_coalesce_with_literal_default() {
        let e = col("x").or_lit(0i64);
        let expected = Expression::coalesce([col("x"), lit(0i64)]);
        assert_eq!(e, expected);
    }

    #[test]
    fn predicate_ext_not_inverts() {
        let inner = col("x").is_null();
        assert_eq!(inner.clone().not(), Predicate::not(inner));
    }
}
