use std::borrow::{Cow, ToOwned};
use std::convert::Infallible;
use std::fmt::Debug;

/// Carrier abstraction for transform outputs.
///
/// A carrier can represent one of two outcomes for a transformed node:
/// - present value (`Cow<'a, T>`)
/// - residual outcome (`Self::Residual`)
///
/// Depending on carrier choice, a residual encodes filtered-out and/or error cases. A non-filtering
/// infallible carrier has `Residual = Infallible`.
///
/// Different carriers encode those states differently:
/// - `()` and `Result<(), E>` are carriers for read-only visitor transforms that return no value
/// - `Cow<'a, T>` and `Result<Cow<'a, T>, E>` are non-filtering carriers
/// - `Option<Cow<'a, T>>` and `Result<Option<Cow<'a, T>>, E>` are filtering carriers
pub trait Carrier<'a, T: ToOwned + ?Sized>: Sized {
    type Residual;

    /// For filtering carriers, Some carrier containing a residual value equivalent to
    /// `Option::None`. None for non-filtering carriers.
    const NONE: Option<Self>;

    /// Wrap a present transformed node.
    fn from_inner(inner: Cow<'a, T>) -> Self;
    /// Wrap a residual outcome for this carrier (e.g. None or Err).
    fn from_residual(residual: Self::Residual) -> Self;
    /// Extracts a present node or returns a residual.
    ///
    /// Filtering and/or fallible carriers encode "filtered out" and "error" in the residual side.
    fn into_inner(self) -> Result<Cow<'a, T>, Self::Residual>;
    /// Extracts an optional node or returns a residual.
    ///
    /// This bridges filtering and non-filtering carriers into a common shape:
    /// - filtering carriers may return `Ok(None)` to indicate "filtered out"
    /// - non-filtering carriers always return `Ok(Some(_))`
    fn into_inner_opt(self) -> Result<Option<Cow<'a, T>>, Self::Residual>;
}

/// Emulates `?` for the result of calling [`Carrier::into_inner_opt`]: If the result is Err,
/// immediately return the residual. Otherwise, unpack the inner value.
macro_rules! carrier_into_inner_opt {
    ($expr:expr) => {
        match $expr.into_inner_opt() {
            Ok(inner) => inner,
            Err(residual) => return Carrier::from_residual(residual),
        }
    };
}
pub(crate) use carrier_into_inner_opt;

/// Attempts to early-return, as if by `None?`, by calling [`Carrier::try_none`]: If the result is
/// Ok(none), immediately return it. Otherwise, do nothing.
macro_rules! carrier_try_none {
    () => {
        if let Some(none) = Carrier::NONE {
            return none;
        }
    };
}
pub(crate) use carrier_try_none;

/// Carrier for infallible non-filtering transforms that can replace but not drop nodes.
impl<'a, T: ToOwned + ?Sized> Carrier<'a, T> for Cow<'a, T> {
    type Residual = Infallible;

    const NONE: Option<Self> = None;

    fn from_inner(inner: Cow<'a, T>) -> Self {
        inner
    }
    fn from_residual(residual: Infallible) -> Self {
        match residual {}
    }
    fn into_inner(self) -> Result<Cow<'a, T>, Infallible> {
        Ok(self)
    }
    fn into_inner_opt(self) -> Result<Option<Cow<'a, T>>, Infallible> {
        Ok(Some(self))
    }
}

/// Carrier for infallible filtering transforms that can drop nodes by returning None.
impl<'a, T: ToOwned + ?Sized> Carrier<'a, T> for Option<Cow<'a, T>> {
    /// The only valid residual value is `()`, which indicates a filtered-out node.
    type Residual = ();

    const NONE: Option<Self> = Some(None);

    fn from_inner(inner: Cow<'a, T>) -> Self {
        Some(inner)
    }
    fn from_residual(_residual: ()) -> Self {
        None
    }
    fn into_inner(self) -> Result<Cow<'a, T>, ()> {
        self.ok_or(())
    }
    fn into_inner_opt(self) -> Result<Option<Cow<'a, T>>, ()> {
        Ok(self)
    }
}

/// Carrier for infallible visitors, which are modeled as always-filter transforms. It silently
/// discards any inner value assigned to it, and is hard-wired to always produce None (drop node).
impl<'a, T: ToOwned + ?Sized> Carrier<'a, T> for () {
    /// The only valid residual value is `()`, which indicates a filtered-out node.
    type Residual = ();

    const NONE: Option<Self> = Some(());

    fn from_inner(_inner: Cow<'a, T>) -> Self {}
    fn from_residual(_residual: ()) -> Self {}
    fn into_inner(self) -> Result<Cow<'a, T>, ()> {
        Err(())
    }
    fn into_inner_opt(self) -> Result<Option<Cow<'a, T>>, ()> {
        Ok(None)
    }
}

/// Carrier for fallible non-filtering transforms, which immediately abort on Err.
impl<'a, T: ToOwned + ?Sized, E: Debug> Carrier<'a, T> for Result<Cow<'a, T>, E> {
    type Residual = E;

    const NONE: Option<Self> = None;

    fn from_inner(inner: Cow<'a, T>) -> Self {
        Ok(inner)
    }
    fn from_residual(residual: E) -> Self {
        Err(residual)
    }
    fn into_inner(self) -> Result<Cow<'a, T>, E> {
        self
    }
    fn into_inner_opt(self) -> Result<Option<Cow<'a, T>>, E> {
        self.map(Some)
    }
}

/// Carrier for fallible filtering transforms, which immediately abort on Err and can also drop
/// nodes by returning None.
impl<'a, T: ToOwned + ?Sized, E: Debug> Carrier<'a, T> for Result<Option<Cow<'a, T>>, E> {
    /// Residual None means drop, Some error means abort
    type Residual = Option<E>;

    const NONE: Option<Self> = Some(Ok(None));

    fn from_inner(inner: Cow<'a, T>) -> Self {
        Ok(Some(inner))
    }
    fn from_residual(residual: Option<E>) -> Self {
        residual.map_or(Ok(None), Err)
    }
    fn into_inner(self) -> Result<Cow<'a, T>, Option<E>> {
        match self {
            Ok(Some(inner)) => Ok(inner),
            Ok(None) => Err(None),
            Err(err) => Err(Some(err)),
        }
    }
    fn into_inner_opt(self) -> Result<Option<Cow<'a, T>>, Option<E>> {
        self.map_err(Some)
    }
}

/// Carrier for fallible visitors, which are modeled as fallible always-filter transforms. It
/// silently discards any inner value assigned to it, and is hard-wired to always produce None
/// unless an error aborts it.
impl<'a, T: ToOwned + ?Sized, E: Debug> Carrier<'a, T> for Result<(), E> {
    /// Residual None means drop, Some error means abort
    type Residual = Option<E>;

    const NONE: Option<Self> = Some(Ok(()));

    fn from_inner(_inner: Cow<'a, T>) -> Self {
        Ok(())
    }
    fn from_residual(residual: Option<E>) -> Self {
        residual.map_or(Ok(()), Err)
    }
    fn into_inner(self) -> Result<Cow<'a, T>, Option<E>> {
        Err(self.err())
    }
    fn into_inner_opt(self) -> Result<Option<Cow<'a, T>>, Option<E>> {
        match self {
            Ok(()) => Ok(None),
            Err(err) => Err(Some(err)),
        }
    }
}
