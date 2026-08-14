use std::borrow::Borrow;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::iter::Peekable;
use std::ops::Deref;

use crate::utils::CollectInto;
use crate::{DeltaResult, Error};

/// A (possibly nested) column name.
///
/// # Construction
///
/// Prefer [`column_name!`] (or [`col!`] / [`column_pred!`] for expressions/predicates) when working
/// with literals, constants, and mixed paths. They are both more concise and more flexible in most
/// cases, e.g.
///
/// ```
/// # use delta_kernel::expressions::{col, column_name, column_pred, ColumnName, Expression, Predicate};
/// assert_eq!(
///     ColumnName::new(["x"]),
///     column_name!("x"),
/// );
///
/// assert_eq!(
///     Expression::column(["x"]),
///     col!("x"),
/// );
///
/// assert_eq!(
///     ColumnName::new(["a", "b", "c"]),
///     column_name!("a.b.c"),
/// );
///
/// assert_eq!(
///     Predicate::column(["a", "b", "c"]),
///     column_pred!("a.b.c"),
/// );
///
/// const FOO: &str = "foo";
/// let bar = "bar";
/// let suffix = column_name!("deeply.nested.leaf");
/// assert_eq!(
///     ColumnName::new([FOO, bar, "baz"].into_iter().chain(suffix.iter().map(String::as_str))),
///     column_name!(FOO, (bar), "baz", ..(suffix)),
/// );
///
/// let middle = column_name!("x.y.z");
/// assert_eq!(
///     ColumnName::new(["a"]).join(&middle).join(["b"]),
///     column_name!("a", ..(middle), "b"),
/// );
/// ```
///
/// Consider using constructors such as [`ColumnName::new`] and [`ColumnName::join`] directly when
/// the macros are a poor fit, e.g.
///
/// ```
/// # use delta_kernel::expressions::{column_name, ColumnName};
/// # let (cond1, cond2) = (false, true);
/// # let path = ["x", "y", "z"];
/// // Path segments generated using complex control flow
/// let mut segments = vec!["start"];
/// if cond1 {
///     segments.push("x");
/// }
/// if cond2 {
///     segments.extend(path.iter().copied());
/// }
/// assert_eq!(
///     ColumnName::new(segments),
///     column_name!(
///         "start",
///         ..(cond1.then_some("x")),
///         ..(cond2.then_some(path).into_iter().flatten()),
///     ),
/// );
///
/// // Joining two existing `ColumnName` instances
/// # let left = column_name!("a.b");
/// # let right = column_name!("c.d");
/// assert_eq!(
///     left.join(&right),
///     column_name!(..(left), ..(right)),
/// );
/// ```
#[derive(Debug, Clone, Default, PartialEq, PartialOrd, Eq, Ord, Serialize, Deserialize)]
pub struct ColumnName {
    path: Vec<String>,
}

impl ColumnName {
    /// Collects path segments into a column name.
    ///
    /// NOTE: This is a low-level constructor; it's usually more convenient to use the
    /// [`column_name!`] family of macros to create column names.
    pub fn new(iter: impl CollectInto<Self>) -> Self {
        iter.collect_into()
    }

    /// Naively splits a string at dots to create a column name.
    ///
    /// This method is _NOT_ recommended for production use, as it does not attempt to interpret
    /// special characters in field names. For example, many systems would interpret the field name
    /// `"a.b" . c ` as equivalent to `ColumnName::new(["\"a.b\"", "c"])` (two fields, whitespace
    /// padding ignored), but this method would return three fields, including whitespace:
    ///
    /// ```
    /// # use delta_kernel::expressions::ColumnName;
    /// assert_eq!(
    ///     ColumnName::from_naive_str_split(" \"a.b\" . c "),
    ///     ColumnName::new([" \"a", "b\" ", " c "])
    /// );
    /// ```
    pub fn from_naive_str_split(name: impl AsRef<str>) -> Self {
        Self::new(name.as_ref().split(FIELD_SEPARATOR))
    }

    /// Parses a comma-separated list of column names, properly accounting for escapes and special
    /// characters, e.g.:
    ///
    /// ```
    /// # use delta_kernel::expressions::ColumnName;
    /// assert_eq!(
    ///     &ColumnName::parse_column_name_list("a.b , c.`d , e` . f").unwrap(),
    ///     &[ColumnName::new(["a", "b"]), ColumnName::new(["c", "d , e", "f"])]
    /// );
    /// ```
    pub fn parse_column_name_list(names: impl AsRef<str>) -> DeltaResult<Vec<ColumnName>> {
        let names = names.as_ref();
        let chars = &mut names.chars().peekable();

        // Ambiguous case: The empty string `""` could reasonably parse as `[ColumnName::new([])]`
        // or `[]`. Prefer the latter as more intuitive and compatible with e.g. `str::join(',')`.
        drop_leading_whitespace(chars);
        let mut ending = match chars.peek() {
            Some(_) => FieldEnding::NextColumn,
            None => FieldEnding::InputExhausted,
        };

        let mut cols = vec![];
        while ending == FieldEnding::NextColumn {
            let (col, new_ending) = parse_column_name(chars)?;
            cols.push(col);
            ending = new_ending;
        }
        Ok(cols)
    }

    /// Concatenates this path with `right`.
    ///
    /// Prefer [`FromIterator`](#impl-FromIterator<ColumnName>-for-ColumnName) when concatenating
    /// multiple [`ColumnName`]s, and prefer the [`column_name!`] family of macros for combining
    /// column names with path parts.
    ///
    /// ```
    /// # use delta_kernel::expressions::{column_name, ColumnName};
    /// let x = column_name!("a.b");
    /// let y = column_name!("c.d");
    /// let z = column_name!("e.f");
    /// assert_eq!(x.join(&y), column_name!("a.b.c.d"));
    /// assert_eq!(ColumnName::from_iter([x.clone(), y, z]), column_name!("a.b.c.d.e.f"));
    /// assert_eq!(column_name!("p", ..(x), "q"), column_name!("p.a.b.q"));
    /// ```
    pub fn join(&self, right: impl CollectInto<ColumnName>) -> ColumnName {
        [self.clone(), right.collect_into()].into_iter().collect()
    }

    /// The path of field names for this column name
    pub fn path(&self) -> &[String] {
        &self.path
    }

    /// Consumes this column name and returns the path of field names.
    pub fn into_inner(self) -> Vec<String> {
        self.path
    }

    /// Returns the parent of this column name, or `None` if this is a top-level column.
    ///
    /// # Examples
    ///
    /// ```
    /// # use delta_kernel::expressions::column_name;
    /// let path = column_name!("user.address.street");
    /// assert_eq!(path.parent(), Some(column_name!("user.address")));
    ///
    /// let path = column_name!("user");
    /// assert_eq!(path.parent(), None);
    /// ```
    pub fn parent(&self) -> Option<ColumnName> {
        if self.path.len() > 1 {
            Some(ColumnName::new(&self.path[..self.path.len() - 1]))
        } else {
            None
        }
    }
}

/// Creates a new column name from a path of field names. Each field name is taken as-is, and may
/// contain arbitrary characters (including periods, spaces, etc.).
impl<A: Into<String>> FromIterator<A> for ColumnName {
    fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
        let path = iter.into_iter().map(|s| s.into()).collect();
        Self { path }
    }
}

/// Joins multiple [`ColumnName`]s into one path. The argument can be any type accepted by
/// [`ColumnName::new`], including `&str` (joining path parts) and `ColumnName` (joining paths).
///
/// NOTE: The [`column_name!`] macro family can express most use cases more clealy.
impl FromIterator<ColumnName> for ColumnName {
    fn from_iter<T: IntoIterator<Item = ColumnName>>(iter: T) -> Self {
        let path = iter.into_iter().flat_map(|c| c.into_iter()).collect();
        Self { path }
    }
}

impl IntoIterator for ColumnName {
    type Item = String;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.path.into_iter()
    }
}

impl<'a> IntoIterator for &'a ColumnName {
    type Item = &'a String;
    type IntoIter = std::slice::Iter<'a, String>;

    fn into_iter(self) -> Self::IntoIter {
        self.path.iter()
    }
}

impl Deref for ColumnName {
    type Target = [String];

    fn deref(&self) -> &[String] {
        &self.path
    }
}

// Allows searching collections of `ColumnName` without an owned key value
impl Borrow<[String]> for ColumnName {
    fn borrow(&self) -> &[String] {
        self
    }
}

// Allows searching collections of `&ColumnName` without an owned key value. Needed because there is
// apparently no blanket `impl<U, T> Borrow<U> for &T where T: Borrow<U>`, even tho `Eq` [1] and
// `Hash` [2] both have blanket impl for treating `&T` like `T`.
//
// [1] https://doc.rust-lang.org/std/cmp/trait.Eq.html#impl-Eq-for-%26A
// [2] https://doc.rust-lang.org/std/hash/trait.Hash.html#impl-Hash-for-%26T
impl Borrow<[String]> for &ColumnName {
    fn borrow(&self) -> &[String] {
        self
    }
}

impl Hash for ColumnName {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        (**self).hash(hasher)
    }
}

/// Formats the column name as a string, with fields delimited by periods. Fields containing special
/// characters are escaped by backtick symbols:
///
/// ```
/// # use delta_kernel::expressions::ColumnName;
/// assert_eq!(ColumnName::new(["a", "b.c", "d"]).to_string(), "a.`b.c`.d");
/// ```
///
/// Backticks inside escaped field names are themselves escaped by doubling:
///
/// ```
/// # use delta_kernel::expressions::ColumnName;
/// assert_eq!(ColumnName::new(["a", "b.`c`.d", "e"]).to_string(), "a.`b.``c``.d`.e");
/// ```
///
/// The string representation is lossless, and can be parsed back into a `ColumnName` using
/// [`FromStr`]:
///
/// ```
/// # use delta_kernel::expressions::ColumnName;
/// let colname = ColumnName::new(["a", "b.c", "d"]);
/// let parsed: ColumnName = colname.to_string().parse().unwrap();
/// assert_eq!(colname, parsed);
/// ```
///
/// [`FromStr`]: std::str::FromStr
impl Display for ColumnName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for (i, s) in self.iter().enumerate() {
            use std::fmt::Write as _;

            // Don't emit a field separator before the first field
            if i > 0 {
                f.write_char(FIELD_SEPARATOR)?;
            }

            let digit_char = |c: char| c.is_ascii_digit();
            if s.is_empty() || s.starts_with(digit_char) || s.contains(|c| !is_simple_char(c)) {
                // Special situation detected. For safety, surround the field name with backticks
                // (with proper escaping if the field name itself contains backticks).
                f.write_char(FIELD_ESCAPE_CHAR)?;
                for c in s.chars() {
                    f.write_char(c)?;
                    if c == FIELD_ESCAPE_CHAR {
                        f.write_char(c)?; // escape the escape by doubling
                    }
                }
                f.write_char(FIELD_ESCAPE_CHAR)?;
            } else {
                // Simple field name -- emit it as-is
                f.write_str(s)?;
            }
        }
        Ok(())
    }
}

// Simple column names contain only simple chars, and do not need to be wrapped in backticks.
pub(crate) fn is_simple_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

fn drop_leading_whitespace(iter: &mut Peekable<impl Iterator<Item = char>>) {
    while iter.next_if(|c| c.is_whitespace()).is_some() {}
}

/// Parses a column name from a string. Field names are separated by dots. Whitespace between fields
/// is ignored. Field names enclosed in backticks may contain arbitrary characters, including
/// periods and spaces. To include a literal backtick in a field name, escape it by doubling, e.g.:
///
/// ```
/// # use delta_kernel::expressions::ColumnName;
/// assert_eq!(ColumnName::new(["a", "b.`c`.d", "e"]).to_string(), "a.`b.``c``.d`.e");
/// ```
///
/// NOTE: Unlike the conversion from `ColumnName` to `String` and back, a conversion from `String`
/// to `ColumnName` and back may not exactly match the original string, if the latter included
/// whitespace or unnecessary field escapes, e.g.:
///
/// ```
/// # use delta_kernel::expressions::ColumnName;
/// let parsed: ColumnName = " `a` . `b.``c``.d` . `e` ".parse().unwrap();
/// assert_eq!(parsed.to_string(), "a.`b.``c``.d`.e");
/// ```
impl std::str::FromStr for ColumnName {
    type Err = Error;

    fn from_str(s: &str) -> DeltaResult<Self> {
        match parse_column_name(&mut s.chars().peekable())? {
            (_, FieldEnding::NextColumn) => Err(Error::generic("Trailing comma in column name")),
            (col, _) => Ok(col),
        }
    }
}

type Chars<'a> = Peekable<std::str::Chars<'a>>;

// What comes after the end of the field we just parsed?
#[derive(PartialEq)]
enum FieldEnding {
    InputExhausted,
    NextField,
    NextColumn,
}

// These characters are remarkably hard to read. Names are a lot less bug-prone.
const FIELD_ESCAPE_CHAR: char = '`';
const FIELD_SEPARATOR: char = '.';
const COLUMN_SEPARATOR: char = ',';

fn parse_column_name(chars: &mut Chars<'_>) -> DeltaResult<(ColumnName, FieldEnding)> {
    // Ambiguous case: The empty string `""`could reasonably parse as either `ColumnName::new([""])`
    // or `ColumnName::new([])`. However, `ColumnName::new([""]).to_string()` is `"[]"` and
    // `ColumnName::new([]).to_string()` is `""`, so we choose the latter because it produces a
    // lossless round trip from `ColumnName` to `String` and back. We also swallow a leading comma
    // to produce an empty column, so that the string "," parses as two empty columns.
    drop_leading_whitespace(chars);
    let mut ending = if chars.peek().is_none() {
        FieldEnding::InputExhausted
    } else if chars.next_if_eq(&COLUMN_SEPARATOR).is_some() {
        FieldEnding::NextColumn
    } else {
        FieldEnding::NextField
    };

    let mut path = vec![];
    while ending == FieldEnding::NextField {
        drop_leading_whitespace(chars);
        let field_name = match chars.next_if_eq(&FIELD_ESCAPE_CHAR) {
            Some(_) => parse_escaped_field_name(chars)?,
            None => parse_simple_field_name(chars)?,
        };

        // Figure out what's next (ignoring leading whitespace)
        ending = match chars.find(|c| !c.is_whitespace()) {
            None => FieldEnding::InputExhausted,
            Some(FIELD_SEPARATOR) => FieldEnding::NextField,
            Some(COLUMN_SEPARATOR) => FieldEnding::NextColumn,
            Some(other) => {
                return Err(Error::generic(format!(
                    "Invalid character {other:?} after field {field_name:?}",
                )))
            }
        };
        path.push(field_name);
    }
    Ok((ColumnName::new(path), ending))
}

/// Parses a simple field name, e.g. 'a.b.c'.
fn parse_simple_field_name(chars: &mut Chars<'_>) -> DeltaResult<String> {
    let mut name = String::new();
    let mut first = true;
    while let Some(c) = chars.next_if(|c| is_simple_char(*c)) {
        if first && c.is_ascii_digit() {
            return Err(Error::generic(format!(
                "Unescaped field name cannot start with a digit {c:?}"
            )));
        }
        name.push(c);
        first = false;
    }
    Ok(name)
}

/// Parses a field name escaped with backticks, e.g. "`ab``c``d`", returning its unescaped logical
/// name. The caller must have already consumed the opening backtick. Shared with the
/// check-constraint tokenizer ([`crate::expressions::sql`]) so backtick-quoted column references
/// parse identically.
/// Examples: `col` -> col;  `ab `` -> ``ab``. Returns an error if there is no closing backtick.
pub(crate) fn parse_escaped_field_name(chars: &mut Chars<'_>) -> DeltaResult<String> {
    let mut name = String::new();
    loop {
        match chars.next() {
            Some(FIELD_ESCAPE_CHAR) if chars.next_if_eq(&FIELD_ESCAPE_CHAR).is_none() => break,
            Some(c) => name.push(c),
            None => {
                return Err(Error::generic(format!(
                    "No closing {FIELD_ESCAPE_CHAR:?} after field {name:?}"
                )));
            }
        }
    }
    Ok(name)
}

/// Validates a single column segment at compile time, returning it unchanged when valid. Used by
/// the `column_name!` proc macro for non-literal segment expressions; the macro unwraps the result
/// in a const context so an invalid segment surfaces as a compile error.
#[doc(hidden)]
pub const fn __require_valid_simple_column_segment(s: &str) -> Option<&str> {
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return None;
    }
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if !b.is_ascii_alphanumeric() && b != b'_' {
            return None;
        }
        i += 1;
    }
    Some(s)
}

/// Builds a [`ColumnName`] from simple path segments (alphanumeric and `_`), with optional
/// runtime interpolation.
///
/// This is the simplest way to construct or manipulate [`ColumnName`] instances, replacing
/// most uses of [`ColumnName::new`] and [`ColumnName::join`].
///
/// Each **string-literal** argument is treated as a dot-separated path and split into
/// segments, so multiple literals concatenate into a single path:
///
/// ```
/// # use delta_kernel::expressions::{column_name, ColumnName};
/// assert_eq!(column_name!("a.b.c"), ColumnName::new(["a", "b", "c"]));
/// assert_eq!(column_name!("a.b", "c.d"), ColumnName::new(["a", "b", "c", "d"]));
/// ```
///
/// Every **constant** argument is taken as a single segment (never split on `.`; its value is
/// not visible at the call site, so a `.` is rejected):
///
/// ```
/// # use delta_kernel::expressions::{column_name, ColumnName};
/// const VERSION: &str = "version";
/// assert_eq!(column_name!(VERSION), ColumnName::new(["version"]));
/// assert_eq!(column_name!(VERSION, "a.b"), ColumnName::new(["version", "a", "b"]));
/// ```
///
/// Runtime values use paren interpolation: `(seg)` inserts one segment (`impl Into<String>`),
/// and `..(path)` splices anything [`ColumnName::new`] accepts (a [`ColumnName`], segment
/// list, etc.). A splice takes its argument by value; borrow with `..(&path)` to keep the
/// source usable afterward:
///
/// ```
/// # use delta_kernel::expressions::{column_name, ColumnName};
/// let leaf = "city";
/// let prefix = column_name!("user.address");
/// assert_eq!(column_name!((leaf)), ColumnName::new(["city"]));
/// // Borrow the prefix so it can be reused below.
/// assert_eq!(
///     column_name!(..(&prefix), (leaf)),
///     ColumnName::new(["user", "address", "city"])
/// );
/// // Final use can move the prefix instead of borrowing.
/// assert_eq!(
///     column_name!("stats_parsed", ..(prefix)),
///     column_name!("stats_parsed.user.address")
/// );
/// ```
///
/// The following would fail to compile:
///
/// ```fail_compile
/// # use delta_kernel::expressions::column_name;
/// let s = "a.b";
/// let name = column_name!(s); // not a constant; use `(s)` for a runtime segment
/// ```
///
/// ```fail_compile
/// # use delta_kernel::expressions::column_name;
/// const BAD: &str = "a.b";
/// let name = column_name!(BAD); // dots not allowed in constant segments
/// ```
///
/// ```fail_compile
/// # use delta_kernel::expressions::column_name;
/// let name = column_name!("a b"); // non-alphanumeric character in path
/// ```
///
/// ```fail_compile
/// # use delta_kernel::expressions::column_name;
/// let name = column_name!("a..b"); // empty segment
/// ```
///
/// ```fail_compile
/// # use delta_kernel::expressions::{column_name, ColumnName};
/// let path = ColumnName::new(["a", "b"]);
/// // A ColumnName is not one segment — splice with `..(path)` instead of `(path)`.
/// let name = column_name!((path));
/// ```
#[doc(inline)]
pub use delta_kernel_derive::column_name;

/// Creates a column [`Expression`](crate::expressions::Expression) by forwarding all args to
/// [`column_name!`]. This is the simplest way to create column name references, replacing almost
/// all uses of [`Expression::column`](crate::expressions::Expression::column).
///
/// ```
/// # use delta_kernel::expressions::{col, ColumnName, Expression};
/// assert_eq!(col!("a.b.c"), Expression::Column(ColumnName::new(["a", "b", "c"])));
///
/// const VERSION: &str = "version";
/// assert_eq!(
///     col!(VERSION, "a.b"),
///     Expression::Column(ColumnName::new(["version", "a", "b"]))
/// );
///
/// let nested = ColumnName::new(["x", "y"]);
/// assert_eq!(col!("add", ..(nested)), Expression::Column(ColumnName::new(["add", "x", "y"])));
/// ```
#[macro_export]
#[doc(hidden)]
macro_rules! __column_expr {
    ( $($name:tt)* ) => {
        $crate::expressions::Expression::from($crate::expressions::column_name!($($name)*))
    };
}
#[doc(hidden)]
pub use __column_expr as column_expr;
#[doc(inline)]
pub use __column_expr as col;

/// Creates an [`ExpressionRef`](crate::expressions::ExpressionRef) by wrapping [`col!`] in
/// [`Arc`](std::sync::Arc). Prefer this over `Arc::new(col!(...))`.
///
/// ```
/// # use std::sync::Arc;
/// # use delta_kernel::expressions::{col, column_expr_ref, ExpressionRef};
/// assert_eq!(column_expr_ref!("a.b"), Arc::new(col!("a.b")));
/// ```
#[macro_export]
#[doc(hidden)]
macro_rules! __column_expr_ref {
    ( $($name:tt)* ) => {
        ::std::sync::Arc::new($crate::expressions::col!($($name)*))
    };
}
#[doc(hidden)]
pub use __column_expr_ref as column_expr_ref;

#[macro_export]
#[doc(hidden)]
macro_rules! __column_pred {
    ( $($name:tt)* ) => {
        $crate::expressions::Predicate::from($crate::expressions::column_name!($($name)*))
    };
}
#[doc(inline)]
pub use __column_pred as column_pred;
use serde::{Deserialize, Serialize};

#[cfg(test)]
mod test {
    use super::*;

    impl ColumnName {
        fn empty() -> Self {
            Self::new(&[] as &[String])
        }
    }

    const TEST_ADD: &str = "add";
    const TEST_STATS: &str = "stats";

    #[test]
    fn test_column_name_macros() {
        let simple = column_name!("x");
        let nested = column_name!("x.y");

        assert_eq!(column_name!("a"), ColumnName::new(["a"]));
        assert_eq!(column_name!("a.b"), ColumnName::new(["a", "b"]));
        assert_eq!(column_name!("a.b.c"), ColumnName::new(["a", "b", "c"]));

        // Every string literal is split on dots, so multiple literals concatenate into one path.
        assert_eq!(
            column_name!("a.b", "c.d"),
            ColumnName::new(["a", "b", "c", "d"])
        );

        assert_eq!(column_name!(TEST_ADD), ColumnName::new(["add"]));
        assert_eq!(
            column_name!(TEST_ADD, TEST_STATS),
            ColumnName::new(["add", "stats"])
        );
        assert_eq!(
            column_name!(TEST_ADD, "parsed"),
            ColumnName::new(["add", "parsed"])
        );
        // A constant segment mixed with a dotted literal path.
        assert_eq!(
            column_name!(TEST_ADD, "a.b"),
            ColumnName::new(["add", "a", "b"])
        );

        // Runtime segment interpolation.
        let leaf = "b";
        assert_eq!(column_name!((leaf)), ColumnName::new(["b"]));
        assert_eq!(column_name!("a", (leaf)), ColumnName::new(["a", "b"]));

        // Path splicing (replaces joined_column_name! / join-of-macro patterns).
        assert_eq!(column_name!(..(&simple), "b"), ColumnName::new(["x", "b"]));
        assert_eq!(
            column_name!(..(&nested), "b"),
            ColumnName::new(["x", "y", "b"])
        );
        assert_eq!(column_name!("a", ..(&simple)), ColumnName::new(["a", "x"]));
        assert_eq!(
            column_name!("a", ..(&nested)),
            ColumnName::new(["a", "x", "y"])
        );
        assert_eq!(
            column_name!("stats_parsed", ..(&nested), (leaf)),
            ColumnName::new(["stats_parsed", "x", "y", "b"])
        );

        // join accepts the same inputs as ColumnName::new.
        assert_eq!(simple.join(["b"]), column_name!("x.b"));
        assert_eq!(nested.join(&simple), column_name!("x.y.x"));
    }

    #[test]
    fn test_column_name_methods() {
        let simple = column_name!("x");
        let nested = column_name!("x.y");

        // path()
        assert_eq!(simple.path(), ["x"]);
        assert_eq!(nested.path(), ["x", "y"]);

        // into_inner()
        assert_eq!(simple.clone().into_inner(), ["x"]);
        assert_eq!(nested.clone().into_inner(), ["x", "y"]);

        // impl Deref
        let name: &[String] = &nested;
        assert_eq!(name, &["x", "y"]);

        // impl<A: Into<String>> FromIterator<A>
        let name: ColumnName = ["x", "y"].into_iter().collect();
        assert_eq!(name, nested);

        // impl FromIterator<ColumnName>
        let name: ColumnName = [&nested, &simple].into_iter().cloned().collect();
        assert_eq!(name, column_name!("x.y.x"));

        // ColumnName::new
        let name = ColumnName::new([nested, simple]);
        assert_eq!(name, column_name!("x.y.x"));

        let name = ColumnName::new(["x", "y"]);
        assert_eq!(name, column_name!("x.y"));

        // ColumnName::into_iter()
        let name = column_name!("x.y.z");
        let name = ColumnName::new(name);
        assert_eq!(name, column_name!("x.y.z"));

        // parent()
        let simple_for_parent = column_name!("x");
        let nested_for_parent = column_name!("x.y");
        assert_eq!(simple_for_parent.parent(), None);
        assert_eq!(nested_for_parent.parent(), Some(column_name!("x")));

        let deep = column_name!("user.address.street");
        assert_eq!(deep.parent(), Some(column_name!("user.address")));

        let single = ColumnName::new(["field"]);
        assert_eq!(single.parent(), None);
    }

    #[test]
    fn test_column_name_from_str() {
        let cases = [
            ("", Some(ColumnName::empty())), // the ambiguous case!
            (".", Some(ColumnName::new(["", ""]))),
            ("  .  ", Some(ColumnName::new(["", ""]))),
            (" ", Some(ColumnName::empty())),
            ("0", None),
            (".a", Some(ColumnName::new(["", "a"]))),
            ("a.", Some(ColumnName::new(["a", ""]))),
            ("  a  .  ", Some(ColumnName::new(["a", ""]))),
            ("a..b", Some(ColumnName::new(["a", "", "b"]))),
            ("`a", None),
            ("a`", None),
            ("a`b`", None),
            ("`a`b", None),
            ("`a``b`", Some(ColumnName::new(["a`b"]))),
            ("  `a``b`  ", Some(ColumnName::new(["a`b"]))),
            ("`a`` b`", Some(ColumnName::new(["a` b"]))),
            ("a", Some(ColumnName::new(["a"]))),
            ("a0", Some(ColumnName::new(["a0"]))),
            ("`a`", Some(ColumnName::new(["a"]))),
            ("  `a`  ", Some(ColumnName::new(["a"]))),
            ("` `", Some(ColumnName::new([" "]))),
            ("  ` `  ", Some(ColumnName::new([" "]))),
            ("`0`", Some(ColumnName::new(["0"]))),
            ("`.`", Some(ColumnName::new(["."]))),
            ("`.`.`.`", Some(ColumnName::new([".", "."]))),
            ("` `.` `", Some(ColumnName::new([" ", " "]))),
            ("a.b", Some(ColumnName::new(["a", "b"]))),
            ("a b", None),
            ("a.`b`", Some(ColumnName::new(["a", "b"]))),
            ("`a`.b", Some(ColumnName::new(["a", "b"]))),
            ("`a`.`b`", Some(ColumnName::new(["a", "b"]))),
            ("`a`.`b`.`c`", Some(ColumnName::new(["a", "b", "c"]))),
            ("`a``.`b```", None),
            ("`a```.`b``", None),
            ("`a```.`b```", Some(ColumnName::new(["a`", "b`"]))),
            ("`a.`b``.c`", None),
            ("`a.``b`.c`", None),
            ("`a.``b``.c`", Some(ColumnName::new(["a.`b`.c"]))),
            ("a`.b``", None),
        ];
        for (input, expected_output) in cases {
            let output: DeltaResult<ColumnName> = input.parse();
            match (&output, &expected_output) {
                (Ok(output), Some(expected_output)) => {
                    assert_eq!(output, expected_output, "from {input}")
                }
                (Err(_), None) => {}
                _ => panic!("Expected {input} to parse as {expected_output:?}, got {output:?}"),
            }
        }
    }

    #[test]
    fn test_column_name_to_string() {
        let cases = [
            ("", ColumnName::empty()), // the ambiguous case!
            ("``.``", ColumnName::new(["", ""])),
            ("``.a", ColumnName::new(["", "a"])),
            ("a.``", ColumnName::new(["a", ""])),
            ("a.``.b", ColumnName::new(["a", "", "b"])),
            ("a", ColumnName::new(["a"])),
            ("a0", ColumnName::new(["a0"])),
            ("`a `", ColumnName::new(["a "])),
            ("` `", ColumnName::new([" "])),
            ("`0`", ColumnName::new(["0"])),
            ("`.`", ColumnName::new(["."])),
            ("`.`.`.`", ColumnName::new([".", "."])),
            ("` `.` `", ColumnName::new([" ", " "])),
            ("a.b", ColumnName::new(["a", "b"])),
            ("a.b.c", ColumnName::new(["a", "b", "c"])),
            ("a.`b.c`.d", ColumnName::new(["a", "b.c", "d"])),
            ("`a```.`b```", ColumnName::new(["a`", "b`"])),
        ];
        for (expected_output, input) in cases {
            let output = input.to_string();
            assert_eq!(output, expected_output);

            let parsed: ColumnName = output.parse().expect(&output);
            assert_eq!(parsed, input);
        }

        // Ensure unnecessary escaping and whitespace is tolerated
        let cases = [
            ("  `a`  ", "a", ColumnName::new(["a"])),
            ("  `a0`  ", "a0", ColumnName::new(["a0"])),
            ("  `a`  .  `b`  ", "a.b", ColumnName::new(["a", "b"])),
        ];
        for (input, expected_output, expected_parsed) in cases {
            let parsed: ColumnName = input.parse().unwrap();
            assert_eq!(parsed, expected_parsed);
            assert_eq!(parsed.to_string(), expected_output);
        }
    }

    #[test]
    fn test_parse_column_name_list() {
        let cases = [
            ("", Some(vec![])),
            (
                "  ,  ",
                Some(vec![ColumnName::empty(), ColumnName::empty()]),
            ),
            ("  a  ", Some(vec![column_name!("a")])),
            (
                "  ,  a  ",
                Some(vec![ColumnName::empty(), column_name!("a")]),
            ),
            (
                "  a  ,  ",
                Some(vec![column_name!("a"), ColumnName::empty()]),
            ),
            ("a  ,  b", Some(vec![column_name!("a"), column_name!("b")])),
            ("`a, b`", Some(vec![ColumnName::new(["a, b"])])),
            ("a.b, c", Some(vec![column_name!("a.b"), column_name!("c")])),
            (
                "`a.b`, c",
                Some(vec![ColumnName::new(["a.b"]), column_name!("c")]),
            ),
            (
                "`a.b`, c",
                Some(vec![ColumnName::new(["a.b"]), column_name!("c")]),
            ),
        ];
        for (input, expected_output) in cases {
            let output = ColumnName::parse_column_name_list(input);
            match (&output, &expected_output) {
                (Ok(output), Some(expected_output)) => {
                    assert_eq!(output, expected_output, "from \"{input}\"")
                }
                (Err(_), None) => {}
                _ => panic!("Expected {input} to parse as {expected_output:?}, got {output:?}"),
            }
        }
    }
}
