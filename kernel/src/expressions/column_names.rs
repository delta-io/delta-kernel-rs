use std::borrow::Borrow;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::ops::Deref;
/// A (possibly nested) column name.
// TODO: Track name as a path rather than a single string
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord)]
pub struct ColumnName {
    path: String,
}

impl ColumnName {
    /// Constructs a new column name from an iterator of field names. The field names are joined
    /// together to make a single path.
    pub fn new(path: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let path: Vec<_> = path.into_iter().map(Into::into).collect();
        let path = path.join(".");
        Self { path }
    }

    /// Joins this column with another, concatenating their fields into a single nested column path.
    ///
    /// NOTE: This is a convenience method that copies two arguments without consuming them. If more
    /// arguments are needed, or if performance is a concern, it is recommended to use
    /// [`FromIterator for ColumnName`](#impl-FromIterator<ColumnName>-for-ColumnName) instead:
    ///
    /// ```
    /// # use delta_kernel::expressions::ColumnName;
    /// let x = ColumnName::new(["a", "b"]);
    /// let y = ColumnName::new(["c", "d"]);
    /// let joined: ColumnName = [x, y].into_iter().collect();
    /// assert_eq!(joined, ColumnName::new(["a", "b", "c", "d"]));
    /// ```
    pub fn join(&self, right: &ColumnName) -> ColumnName {
        [self.clone(), right.clone()].into_iter().collect()
    }

    /// The path of field names for this column name
    pub fn path(&self) -> &String {
        &self.path
    }

    /// Consumes this column name and returns the path of field names.
    pub fn into_inner(self) -> String {
        self.path
    }
}

/// Creates a new column name from a path of field names. Each field name is taken as-is, and may
/// contain arbitrary characters (including periods, spaces, etc.).
impl<A: Into<String>> FromIterator<A> for ColumnName {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = A>,
    {
        Self::new(iter)
    }
}

/// Creates a new column name by joining multiple column names together.
impl FromIterator<ColumnName> for ColumnName {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = ColumnName>,
    {
        Self::new(iter.into_iter().map(ColumnName::into_inner))
    }
}

impl Display for ColumnName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        (**self).fmt(f)
    }
}

impl Deref for ColumnName {
    type Target = String;

    fn deref(&self) -> &String {
        &self.path
    }
}

// Allows searching collections of `ColumnName` without an owned key value
impl Borrow<String> for ColumnName {
    fn borrow(&self) -> &String {
        self
    }
}

// Allows searching collections of `&ColumnName` without an owned key value. Needed because there is
// apparently no blanket `impl<U, T> Borrow<U> for &T where T: Borrow<U>`, even tho `Eq` [1] and
// `Hash` [2] both have blanket impl for treating `&T` like `T`.
//
// [1] https://doc.rust-lang.org/std/cmp/trait.Eq.html#impl-Eq-for-%26A
// [2] https://doc.rust-lang.org/std/hash/trait.Hash.html#impl-Hash-for-%26T
impl Borrow<String> for &ColumnName {
    fn borrow(&self) -> &String {
        self
    }
}

impl Hash for ColumnName {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        (**self).hash(hasher)
    }
}

/// Creates a nested column name whose field names are all simple column names (containing only
/// alphanumeric characters and underscores), delimited by dots. This macro is provided as a
/// convenience for the common case where the caller knows the column name contains only simple
/// field names and that splitting by periods is safe:
///
/// ```
/// # use delta_kernel::expressions::{column_name, ColumnName};
/// assert_eq!(column_name!("a.b.c"), ColumnName::new(["a", "b", "c"]));
/// ```
///
/// To avoid accidental misuse, the argument must be a string literal, so the compiler can validate
/// the safety conditions. Thus, the following uses would fail to compile:
///
/// ```fail_compile
/// # use delta_kernel::expressions::column_name;
/// let s = "a.b";
/// let name = column_name!(s); // not a string literal
/// ```
///
/// ```fail_compile
/// # use delta_kernel::expressions::simple_column_name;
/// let name = simple_column_name!("a b"); // non-alphanumeric character
/// ```
// NOTE: Macros are only public if exported, which defines them at the root of the crate. But we
// don't want it there. So, we export a hidden macro and pub use it here where we actually want it.
#[macro_export]
#[doc(hidden)]
macro_rules! __column_name {
    ( $($name:tt)* ) => {
        $crate::expressions::ColumnName::new(delta_kernel_derive::parse_column_name!($($name)*))
    };
}
#[doc(inline)]
pub use __column_name as column_name;

/// Joins two column names together, when one or both inputs might be literal strings representing
/// simple (non-nested) column names. For example:
///
/// ```
/// # use delta_kernel::expressions::{column_name, joined_column_name};
/// assert_eq!(joined_column_name!("a.b", "c"), column_name!("a.b").join(&column_name!("c")))
/// ```
///
/// To avoid accidental misuse, at least one argument must be a string literal. Thus, the following
/// invocation would fail to compile:
///
/// ```fail_compile
/// # use delta_kernel::expressions::joined_column_name;
/// let s = "s";
/// let name = joined_column_name!(s, s);
/// ```
#[macro_export]
#[doc(hidden)]
macro_rules! __joined_column_name {
    ( $left:literal, $right:literal ) => {
        $crate::__column_name!($left).join(&$crate::__column_name!($right))
    };
    ( $left:literal, $right:expr ) => {
        $crate::__column_name!($left).join(&$right)
    };
    ( $left:expr, $right:literal) => {
        $left.join(&$crate::__column_name!($right))
    };
    ( $($other:tt)* ) => {
        compile_error!("joined_column_name!() requires at least one string literal input")
    };
}
#[doc(inline)]
pub use __joined_column_name as joined_column_name;

#[macro_export]
#[doc(hidden)]
macro_rules! __column_expr {
    ( $($name:tt)* ) => {
        $crate::expressions::Expression::from($crate::__column_name!($($name)*))
    };
}
#[doc(inline)]
pub use __column_expr as column_expr;

#[macro_export]
#[doc(hidden)]
macro_rules! __joined_column_expr {
    ( $($name:tt)* ) => {
        $crate::expressions::Expression::from($crate::__joined_column_name!($($name)*))
    };
}
#[doc(inline)]
pub use __joined_column_expr as joined_column_expr;

#[cfg(test)]
mod test {
    use super::*;
    use delta_kernel_derive::parse_column_name;

    #[test]
    fn test_parse_column_name_macros() {
        assert_eq!(parse_column_name!("a"), ["a"]);

        assert_eq!(parse_column_name!("a"), ["a"]);
        assert_eq!(parse_column_name!("a.b"), ["a", "b"]);
        assert_eq!(parse_column_name!("a.b.c"), ["a", "b", "c"]);
    }

    #[test]
    fn test_column_name_macros() {
        let simple = column_name!("x");
        let nested = column_name!("x.y");

        assert_eq!(column_name!("a"), ColumnName::new(["a"]));
        assert_eq!(column_name!("a.b"), ColumnName::new(["a", "b"]));
        assert_eq!(column_name!("a.b.c"), ColumnName::new(["a", "b", "c"]));

        assert_eq!(joined_column_name!("a", "b"), ColumnName::new(["a", "b"]));
        assert_eq!(joined_column_name!("a", "b"), ColumnName::new(["a", "b"]));

        assert_eq!(
            joined_column_name!(simple, "b"),
            ColumnName::new(["x", "b"])
        );
        assert_eq!(
            joined_column_name!(nested, "b"),
            ColumnName::new(["x.y", "b"])
        );

        assert_eq!(
            joined_column_name!("a", &simple),
            ColumnName::new(["a", "x"])
        );
        assert_eq!(
            joined_column_name!("a", &nested),
            ColumnName::new(["a", "x.y"])
        );
    }

    #[test]
    fn test_column_name_methods() {
        let simple = column_name!("x");
        let nested = column_name!("x.y");

        // path()
        assert_eq!(simple.path(), "x");
        assert_eq!(nested.path(), "x.y");

        // into_inner()
        assert_eq!(simple.clone().into_inner(), "x");
        assert_eq!(nested.clone().into_inner(), "x.y");

        // impl Deref
        let name: &str = &nested;
        assert_eq!(name, "x.y");

        // impl<A: Into<String>> FromIterator<A>
        let name: ColumnName = ["x", "y"].into_iter().collect();
        assert_eq!(name, nested);

        // impl FromIterator<ColumnName>
        let name: ColumnName = [nested, simple].into_iter().collect();
        assert_eq!(name, column_name!("x.y.x"));
    }
}
