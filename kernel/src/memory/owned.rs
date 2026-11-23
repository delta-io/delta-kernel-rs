use std::ops::{Deref, DerefMut};

use allocator_api2::boxed::Box as AllocBox;

use super::allocator::ContainerAllocator;
use super::{MemoryResult, MetadataVec};

/// An owned metadata payload using the shared Kernel allocator.
///
/// Use [`Self::try_new`] at fallible boundaries. Infallible conversions and cloning have Rust's
/// ordinary allocation-error behavior if the allocator rejects their allocation.
pub struct Owned<T>(AllocBox<T, ContainerAllocator>);

impl<T> Owned<T> {
    /// Allocate a payload, returning an error if its allocation cannot be reserved.
    pub fn try_new(value: T) -> MemoryResult<Self> {
        let mut values = MetadataVec::new();
        values.try_reserve_exact(1)?;
        values.try_push(value)?;
        // Exact capacity avoids a potentially allocating shrink during boxing.
        let (ptr, allocator) =
            AllocBox::into_raw_with_allocator(values.into_vec().into_boxed_slice());
        // SAFETY: A single-element slice has its element's layout and preserves the allocator.
        Ok(Self(unsafe {
            AllocBox::from_raw_in(ptr.cast::<T>(), allocator)
        }))
    }

    /// Move out the value and release its tracked allocation.
    pub fn into_inner(self) -> T {
        AllocBox::into_inner(self.0)
    }
}
impl<T> From<T> for Owned<T> {
    fn from(value: T) -> Self {
        Self(AllocBox::new_in(value, ContainerAllocator))
    }
}
impl<T> From<Box<T>> for Owned<T> {
    fn from(value: Box<T>) -> Self {
        Self::from(*value)
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
