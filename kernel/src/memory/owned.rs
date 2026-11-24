use std::ops::{Deref, DerefMut};

use super::{MemoryResult, storage};

/// An owned metadata payload using the shared Kernel allocator.
///
/// Use [`Self::try_new`] at fallible boundaries. Infallible conversions and cloning have Rust's
/// ordinary allocation-error behavior if the allocator rejects their allocation.
pub struct Owned<T>(storage::Owned<T>);

impl<T> Owned<T> {
    /// Allocate a payload, returning an error if its allocation cannot be reserved.
    pub fn try_new(value: T) -> MemoryResult<Self> {
        storage::try_boxed(value).map(Self)
    }

    /// Move out the value and release its tracked allocation.
    pub fn into_inner(self) -> T {
        *self.0
    }
}
impl<T> From<T> for Owned<T> {
    fn from(value: T) -> Self {
        Self(storage::boxed(value))
    }
}
impl<T> From<Box<T>> for Owned<T> {
    fn from(value: Box<T>) -> Self {
        #[cfg(feature = "memory-budget")]
        return Self::from(*value);
        #[cfg(not(feature = "memory-budget"))]
        Self(value)
    }
}
impl<T: Clone> Clone for Owned<T> {
    fn clone(&self) -> Self {
        Self::from((**self).clone())
    }
}
impl<T> Deref for Owned<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}
impl<T> DerefMut for Owned<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}
impl<T: PartialEq> PartialEq for Owned<T> {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}
impl<T: Eq> Eq for Owned<T> {}
impl<T> AsRef<T> for Owned<T> {
    fn as_ref(&self) -> &T {
        self
    }
}
impl<T> AsMut<T> for Owned<T> {
    fn as_mut(&mut self) -> &mut T {
        self
    }
}
impl<T: serde::Serialize> serde::Serialize for Owned<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        (**self).serialize(serializer)
    }
}
impl<T: std::fmt::Display> std::fmt::Display for Owned<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        (**self).fmt(f)
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for Owned<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        (**self).fmt(f)
    }
}
