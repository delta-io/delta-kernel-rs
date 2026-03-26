use std::borrow::{Cow, ToOwned};

mod expression;
mod schema;
pub use self::expression::{ExpressionDepthChecker, ExpressionTransform};
pub use self::schema::{SchemaDepthChecker, SchemaTransform};

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

/// Rebuilds a two-child parent from transformed children only when needed.
///
/// If either child is filtered out (`None`), filter out the parent by returning `None`. If both children survive as
/// borrowed values, this returns a borrowed parent. Otherwise, it uses the provided `map_owned` function to rebuild and return an owned
/// parent.
pub(crate) fn map_owned_pair_or_else<'a, Parent, Child>(
    parent: &'a Parent,
    left: Option<Cow<'a, Child>>,
    right: Option<Cow<'a, Child>>,
    map_owned: impl FnOnce((Child::Owned, Child::Owned)) -> Parent,
) -> Option<Cow<'a, Parent>>
where
    Parent: Clone,
    Child: ToOwned + ?Sized + 'a,
{
    let (Some(left), Some(right)) = (left, right) else {
        return None;
    };
    Some(match (left, right) {
        (Cow::Borrowed(_), Cow::Borrowed(_)) => Cow::Borrowed(parent),
        (left, right) => Cow::Owned(map_owned((left.into_owned(), right.into_owned()))),
    })
}

/// Rebuilds a single-child parent from a transformed child only when needed.
///
/// If the child is filtered out (`None`), filter out the parent by returning `None`. If the child
/// survives as a borrowed value, this returns a borrowed parent. Otherwise, it uses the provided
/// `map_owned` function to rebuild and return an owned parent.
pub(crate) fn map_owned_or_else<'a, Parent, Child>(
    parent: &'a Parent,
    child: Option<Cow<'a, Child>>,
    map_owned: impl FnOnce(Child::Owned) -> Parent,
) -> Option<Cow<'a, Parent>>
where
    Parent: Clone,
    Child: ToOwned + ?Sized + 'a,
{
    Some(match child? {
        Cow::Owned(v) => Cow::Owned(map_owned(v)),
        Cow::Borrowed(_) => Cow::Borrowed(parent),
    })
}
