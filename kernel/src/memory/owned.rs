use std::ops::{Deref, DerefMut};

use super::{LimitedAllocator, LimitedVec, MemoryBudget, MemoryResult};

/// Owned payload storage with a fallible allocator-aware construction path.
///
/// Infallible `Clone` retains its ordinary allocation semantics outside the cap. Code that must
/// enforce a budget must construct its replacement through `try_new` instead.
pub struct Owned<T>(Storage<T>);

#[derive(Debug)]
enum Storage<T> {
    Global(Box<T>),
    Limited(Box<T, LimitedAllocator>),
}

impl<T: std::fmt::Debug> std::fmt::Debug for Owned<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        (**self).fmt(f)
    }
}

impl<T> Owned<T> {
    /// Construct owned storage under the active budget, if configured.
    pub fn try_new(value: T) -> MemoryResult<Self> {
        match MemoryBudget::current() {
            Some(budget) => Self::try_new_in(value, budget.allocator()),
            None => Ok(Self::unbudgeted(value)),
        }
    }

    /// Construct owned storage using an explicit allocator without infallible allocation.
    pub fn try_new_in(value: T, allocator: LimitedAllocator) -> MemoryResult<Self> {
        let mut values = LimitedVec::new_in(allocator);
        values.try_reserve_exact(1)?;
        values.try_push(value)?;
        // The single-element vector has no spare capacity, so boxing cannot require a shrink.
        let slice = values.into_vec().into_boxed_slice();
        let (ptr, allocator) = Box::into_raw_with_allocator(slice);
        // SAFETY: A one-element boxed slice has the same data layout as its element. Allocator
        // ownership and the original allocation's size and alignment are preserved.
        let boxed = unsafe { Box::from_raw_in(ptr.cast::<T>(), allocator) };
        Ok(Self(Storage::Limited(boxed)))
    }

    pub(crate) fn unbudgeted(value: T) -> Self {
        Self(Storage::Global(Box::new(value)))
    }

    /// Move out the value, releasing its allocation and reservation.
    pub fn into_inner(self) -> T {
        match self.0 {
            Storage::Global(value) => *value,
            Storage::Limited(value) => *value,
        }
    }
}

impl<T> Deref for Owned<T> {
    type Target = T;
    fn deref(&self) -> &T {
        match &self.0 {
            Storage::Global(value) => value,
            Storage::Limited(value) => value,
        }
    }
}
impl<T> DerefMut for Owned<T> {
    fn deref_mut(&mut self) -> &mut T {
        match &mut self.0 {
            Storage::Global(value) => value,
            Storage::Limited(value) => value,
        }
    }
}
impl<T: Clone> Clone for Owned<T> {
    fn clone(&self) -> Self {
        Self::unbudgeted((**self).clone())
    }
}
impl<T: PartialEq> PartialEq for Owned<T> {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}
impl<T: Eq> Eq for Owned<T> {}
impl<T> From<T> for Owned<T> {
    fn from(value: T) -> Self {
        Self::unbudgeted(value)
    }
}
impl<T> From<Box<T>> for Owned<T> {
    fn from(value: Box<T>) -> Self {
        Self(Storage::Global(value))
    }
}
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
