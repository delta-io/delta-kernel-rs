//! Shared transform infrastructure.
use std::borrow::{Cow, ToOwned};

mod carrier;
mod expression;
mod schema;

pub use self::expression::{ExpressionDepthChecker, ExpressionTransform};
pub use self::schema::{SchemaDepthChecker, SchemaTransform};

pub use carrier::Carrier;
pub(crate) use carrier::{carrier_into_inner_opt, carrier_try_none};

/// Defines a transform's `Output` and `Residual` associated types.
///
/// Example: fallible schema visitor
/// ```rust,no_run
/// # use delta_kernel::transform_output_type;
/// # use delta_kernel::schema::{SchemaTransform, StructField};
/// # use delta_kernel::DeltaResult;
/// struct Validate;
///
/// impl<'a> SchemaTransform<'a> for Validate {
///     transform_output_type!(|'a, T| DeltaResult<()>);
///
///     fn transform_struct_field(&mut self, _field: &'a StructField) -> DeltaResult<()> {
///         todo!()
///     }
/// }
/// ```
///
/// Example: infallible filtering expression transform
/// ```rust,no_run
/// # use std::borrow::Cow;
/// # use delta_kernel::transform_output_type;
/// # use delta_kernel::expressions::ColumnName;
/// # use delta_kernel::transforms::ExpressionTransform;
/// struct KeepSomeColumns;
///
/// impl<'a> ExpressionTransform<'a> for KeepSomeColumns {
///     transform_output_type!(|'a, T| Option<Cow<'a, T>>);
///
///     fn transform_expr_column(&mut self, _name: &'a ColumnName) -> Option<Cow<'a, ColumnName>> {
///         todo!()
///     }
/// }
/// ```
#[macro_export]
macro_rules! transform_output_type {
    (|$lt:lifetime, $T:ident| $out:ty) => {
        type Output<$T: ::std::borrow::ToOwned + ?Sized + $lt> = $out;
        type Residual = <Self::Output<()> as $crate::transforms::Carrier<$lt, ()>>::Residual;
    };
}
#[doc(inline)]
pub use transform_output_type;

/// Rebuilds a parent from transformed children only when needed.
///
/// This helper consumes transformed child carriers. When a filtering carrier is used and all
/// children were filtered out, filter out the parent as well. If all children were unchanged, it
/// returns a carrier for the original parent. Otherwise returns a carrier for a new owned parent,
/// produced by the provided `map_owned`.
pub(crate) fn map_owned_children_or_else<'a, Parent, Child, ChildCarrier, ParentCarrier, R>(
    parent: &'a Parent,
    children: impl ExactSizeIterator<Item = ChildCarrier>,
    map_owned: impl FnOnce(Vec<<Child as ToOwned>::Owned>) -> <Parent as ToOwned>::Owned,
) -> ParentCarrier
where
    Parent: ToOwned + ?Sized,
    Child: ToOwned + ?Sized + 'a,
    ChildCarrier: Carrier<'a, Child, Residual = R>,
    ParentCarrier: Carrier<'a, Parent, Residual = R>,
{
    let num_children = children.len();
    let mut num_borrowed = 0;

    // NOTE: A transform in visitor mode uses the ZST () as Carrier (= vec allocates no memory),
    let mut new_children = Vec::with_capacity(num_children);
    for child in children {
        if let Some(transformed) = carrier_into_inner_opt!(child) {
            if let Cow::Borrowed(_) = transformed {
                num_borrowed += 1;
            }
            new_children.push(transformed);
        }
    }

    // If all children were filtered out, try to return the "None" carrier. If not supported, then
    // the parent must have already been empty and it's safe to continue (return borrowed parent).
    if new_children.is_empty() {
        carrier_try_none!();
    }

    if num_borrowed < num_children {
        let owned = new_children.into_iter().map(Cow::into_owned).collect();
        Carrier::from_inner(Cow::Owned(map_owned(owned)))
    } else {
        Carrier::from_inner(Cow::Borrowed(parent))
    }
}

/// Rebuilds a two-child parent from transformed children only when needed.
///
/// If either child is filtered out (`None`), filter out the parent by returning `None`. If both children survive as
/// borrowed values, this returns a borrowed parent. Otherwise, it uses the provided `map_owned` function to rebuild and return an owned
/// parent.
pub(crate) fn map_owned_pair_or_else<'a, Parent, Child, ChildCarrier, ParentCarrier, R>(
    parent: &'a Parent,
    left: ChildCarrier,
    right: ChildCarrier,
    map_owned: impl FnOnce((Child::Owned, Child::Owned)) -> Parent,
) -> ParentCarrier
where
    Parent: Clone,
    Child: ToOwned + ?Sized + 'a,
    ChildCarrier: Carrier<'a, Child, Residual = R>,
    ParentCarrier: Carrier<'a, Parent, Residual = R>,
{
    let left = carrier_into_inner_opt!(left);
    let right = carrier_into_inner_opt!(right);
    let (Some(left), Some(right)) = (left, right) else {
        // SAFETY: Only a filtering carrier could produce None => try_none must succeed.
        carrier_try_none!();
        unreachable!();
    };
    Carrier::from_inner(match (left, right) {
        (Cow::Borrowed(_), Cow::Borrowed(_)) => Cow::Borrowed(parent),
        (left, right) => Cow::Owned(map_owned((left.into_owned(), right.into_owned()))),
    })
}

/// Rebuilds a single-child parent from a transformed child only when needed.
///
/// If the child is filtered out (`None`), filter out the parent by returning `None`. If the child
/// survives as a borrowed value, this returns a borrowed parent. Otherwise, it uses the provided
/// `map_owned` function to rebuild and return an owned parent.
pub(crate) fn map_owned_or_else<'a, Parent, Child, ChildCarrier, ParentCarrier, R>(
    parent: &'a Parent,
    child: ChildCarrier,
    map_owned: impl FnOnce(Child::Owned) -> Parent,
) -> ParentCarrier
where
    Parent: Clone,
    Child: ToOwned + ?Sized + 'a,
    ChildCarrier: Carrier<'a, Child, Residual = R>,
    ParentCarrier: Carrier<'a, Parent, Residual = R>,
{
    let child = carrier_into_inner_opt!(child);
    let Some(child) = child else {
        // SAFETY: Only a filtering carrier could produce None => try_none must succeed.
        carrier_try_none!();
        unreachable!();
    };
    Carrier::from_inner(match child {
        Cow::Owned(v) => Cow::Owned(map_owned(v)),
        Cow::Borrowed(_) => Cow::Borrowed(parent),
    })
}
