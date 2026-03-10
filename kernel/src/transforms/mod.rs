use std::borrow::Cow;
use std::borrow::ToOwned;

mod expression;
mod schema;
pub use self::expression::{ExpressionDepthChecker, ExpressionTransform};
pub use self::schema::{SchemaDepthChecker, SchemaTransform};

// Extension trait for Cow<'_, T>
pub(crate) trait CowExt<T: ToOwned + ?Sized> {
    /// The owned type that corresopnds to Self
    type Owned;

    /// Propagate the results of nested transforms. If the nested transform made no change (borrowed
    /// `self`), then return a borrowed result `s` as well. Otherwise, invoke the provided mapping
    /// function `f` to convert the owned nested result into an owned result.
    fn map_owned_or_else<S: Clone>(self, s: &S, f: impl FnOnce(Self::Owned) -> S) -> Cow<'_, S>;
}

// Basic implementation for a single Cow value
impl<T: ToOwned + ?Sized> CowExt<T> for Cow<'_, T> {
    type Owned = T::Owned;

    fn map_owned_or_else<S: Clone>(self, s: &S, f: impl FnOnce(T::Owned) -> S) -> Cow<'_, S> {
        match self {
            Cow::Owned(v) => Cow::Owned(f(v)),
            Cow::Borrowed(_) => Cow::Borrowed(s),
        }
    }
}

// Additional implementation for a pair of Cow values
impl<'a, T: ToOwned + ?Sized> CowExt<(Cow<'a, T>, Cow<'a, T>)> for (Cow<'a, T>, Cow<'a, T>) {
    type Owned = (T::Owned, T::Owned);

    fn map_owned_or_else<S: Clone>(self, s: &S, f: impl FnOnce(Self::Owned) -> S) -> Cow<'_, S> {
        match self {
            (Cow::Borrowed(_), Cow::Borrowed(_)) => Cow::Borrowed(s),
            (left, right) => Cow::Owned(f((left.into_owned(), right.into_owned()))),
        }
    }
}

/// Rebuilds a parent from transformed children only when needed.
///
/// Child transforms may filter nodes by returning `None`. If all children are filtered out, this
/// returns `None`. If all original children survive as borrowed values, this returns a borrowed
/// parent. Otherwise, it rebuilds and returns an owned parent.
pub(crate) fn map_owned_children_or_else<'a, Parent, Child>(
    parent: &'a Parent,
    children: impl ExactSizeIterator<Item = Option<Cow<'a, Child>>>,
    map_owned: impl FnOnce(Vec<Child::Owned>) -> Parent::Owned,
) -> Option<Cow<'a, Parent>>
where
    Parent: ToOwned + ?Sized,
    Child: ToOwned + ?Sized + 'a,
{
    let num_children = children.len();
    let mut num_borrowed = 0;
    let mut new_children = Vec::with_capacity(num_children);
    for child in children.flatten() {
        if let Cow::Borrowed(_) = child {
            num_borrowed += 1;
        }
        new_children.push(child);
    }

    if new_children.is_empty() {
        None
    } else if num_borrowed < num_children {
        let owned = new_children.into_iter().map(Cow::into_owned).collect();
        Some(Cow::Owned(map_owned(owned)))
    } else {
        Some(Cow::Borrowed(parent))
    }
}
