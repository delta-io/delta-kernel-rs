use std::alloc::{Layout, alloc};
use std::ops::{Deref, DerefMut};

use super::{KERNEL_ALLOCATOR, MemoryResult};

/// An owned metadata payload using the shared Kernel allocator.
///
/// Use [`Self::try_new`] at fallible boundaries. Infallible conversions and cloning have Rust's
/// ordinary allocation-error behavior if the allocator rejects their allocation.
pub struct Owned<T>(Box<T>);

impl<T> Owned<T> {
    /// Allocate a payload, returning an error if its allocation cannot be reserved.
    pub fn try_new(value: T) -> MemoryResult<Self> {
        let layout = Layout::new::<T>();
        if layout.size() == 0 {
            return Ok(Self(Box::new(value)));
        }
        // SAFETY: The nonzero layout describes T. Box takes ownership only after initialization,
        // using the same process allocator for its eventual deallocation.
        let pointer = unsafe { alloc(layout) }.cast::<T>();
        if pointer.is_null() {
            return Err(KERNEL_ALLOCATOR.allocation_error());
        }
        unsafe {
            pointer.write(value);
            Ok(Self(Box::from_raw(pointer)))
        }
    }

    /// Move out the value and release its tracked allocation.
    pub fn into_inner(self) -> T {
        *self.0
    }
}
impl<T> From<T> for Owned<T> {
    fn from(value: T) -> Self {
        Self(Box::new(value))
    }
}
impl<T> From<Box<T>> for Owned<T> {
    fn from(value: Box<T>) -> Self {
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
