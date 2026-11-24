#[cfg(feature = "memory-budget")]
use std::alloc::Allocator;
use std::alloc::Layout;

#[cfg(feature = "memory-budget")]
use super::{KERNEL_ALLOCATOR, KernelAllocator};
use super::{MemoryResult, allocation_error};

#[cfg(feature = "memory-budget")]
pub(super) type Vector<T> = Vec<T, KernelAllocator>;
#[cfg(not(feature = "memory-budget"))]
pub(super) type Vector<T> = Vec<T>;

pub(super) type IntoIter<T> = <Vector<T> as IntoIterator>::IntoIter;

#[cfg(feature = "memory-budget")]
pub(super) type Owned<T> = Box<T, KernelAllocator>;
#[cfg(not(feature = "memory-budget"))]
pub(super) type Owned<T> = Box<T>;

pub(super) fn vector<T>() -> Vector<T> {
    #[cfg(feature = "memory-budget")]
    return Vec::new_in(KERNEL_ALLOCATOR);
    #[cfg(not(feature = "memory-budget"))]
    Vec::new()
}

pub(super) fn from_vector<T>(values: Vec<T>) -> Vector<T> {
    #[cfg(feature = "memory-budget")]
    {
        let mut storage = vector();
        storage.extend(values);
        storage
    }
    #[cfg(not(feature = "memory-budget"))]
    values
}

pub(super) fn boxed<T>(value: T) -> Owned<T> {
    #[cfg(feature = "memory-budget")]
    return Box::new_in(value, KERNEL_ALLOCATOR);
    #[cfg(not(feature = "memory-budget"))]
    Box::new(value)
}

pub(super) fn try_boxed<T>(value: T) -> MemoryResult<Owned<T>> {
    let layout = Layout::new::<T>();
    if layout.size() == 0 {
        return Ok(boxed(value));
    }
    #[cfg(feature = "memory-budget")]
    let pointer = KERNEL_ALLOCATOR
        .allocate(layout)
        .map_err(|_| allocation_error())?
        .cast::<T>()
        .as_ptr();
    #[cfg(not(feature = "memory-budget"))]
    let pointer = {
        // SAFETY: The nonzero layout describes T; Box owns the initialized allocation below.
        let pointer = unsafe { std::alloc::alloc(layout) }.cast::<T>();
        if pointer.is_null() {
            return Err(allocation_error());
        }
        pointer
    };
    // SAFETY: The block has T's layout and is initialized before transferring ownership to Box.
    // Each backend retains the same allocator for eventual destruction.
    unsafe {
        pointer.write(value);
        #[cfg(feature = "memory-budget")]
        return Ok(Box::from_raw_in(pointer, KERNEL_ALLOCATOR));
        #[cfg(not(feature = "memory-budget"))]
        Ok(Box::from_raw(pointer))
    }
}
