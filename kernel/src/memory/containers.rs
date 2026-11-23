use std::io::{self, Write};
use std::ops::{Deref, DerefMut};

use allocator_api2::vec::Vec as AllocVec;
use serde::Serialize;

use super::allocator::ContainerAllocator;
use super::{KERNEL_ALLOCATOR, MemoryError, MemoryResult};

/// An allocator-aware vector with fallible growth methods.
///
/// The backing allocation is charged by capacity. Allocations owned by `T` must independently
/// use the allocator; an ordinary `String` here does not charge its contents. Infallible
/// conversions and cloning retain Rust's ordinary allocation-error behavior on exhaustion.
#[derive(Debug)]
pub struct MetadataVec<T>(AllocVec<T, ContainerAllocator>);

impl<T> MetadataVec<T> {
    /// Create empty storage using the shared Kernel allocator.
    pub fn new() -> Self {
        Self(AllocVec::new_in(ContainerAllocator))
    }

    /// Append `value`, returning an error without changing the elements if growth fails.
    pub fn try_push(&mut self, value: T) -> MemoryResult<()> {
        self.try_reserve(1)?;
        self.0.push(value);
        Ok(())
    }

    /// Reserve space using geometric growth; existing and replacement blocks overlap in the cap.
    pub fn try_reserve(&mut self, additional: usize) -> MemoryResult<()> {
        self.check_capacity(additional)?;
        self.0
            .try_reserve(additional)
            .map_err(|_| self.allocation_error())
    }

    /// Reserve only the requested capacity, useful when the final element count is known.
    pub fn try_reserve_exact(&mut self, additional: usize) -> MemoryResult<()> {
        self.check_capacity(additional)?;
        self.0
            .try_reserve_exact(additional)
            .map_err(|_| self.allocation_error())
    }

    /// Remove and return the last element, retaining charged vector capacity.
    pub fn pop(&mut self) -> Option<T> {
        self.0.pop()
    }

    /// Drop all elements, retaining charged vector capacity.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Shorten the vector, retaining charged capacity.
    pub fn truncate(&mut self, len: usize) {
        self.0.truncate(len);
    }

    /// Borrow elements mutably without exposing infallible vector growth.
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.0
    }

    /// Return the number of elements the current allocation can hold.
    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    pub(super) fn into_vec(self) -> AllocVec<T, ContainerAllocator> {
        self.0
    }

    fn check_capacity(&self, additional: usize) -> MemoryResult<()> {
        let len = self
            .len()
            .checked_add(additional)
            .ok_or(MemoryError::CapacityOverflow)?;
        if size_of::<T>() != 0 && len > isize::MAX as usize / size_of::<T>() {
            return Err(MemoryError::CapacityOverflow);
        }
        Ok(())
    }

    fn allocation_error(&self) -> MemoryError {
        KERNEL_ALLOCATOR.allocation_error()
    }
}

impl<T> Deref for MetadataVec<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// An owned byte buffer whose capacity uses the shared Kernel allocator.
pub type MetadataBytes = MetadataVec<u8>;

impl MetadataBytes {
    /// Copy bytes into budgeted storage without mutating the buffer if growth fails.
    pub fn try_extend_from_slice(&mut self, bytes: &[u8]) -> MemoryResult<()> {
        self.try_reserve(bytes.len())?;
        self.0.extend_from_slice(bytes);
        Ok(())
    }

    /// Copy a slice into a buffer with exactly requested initial capacity.
    pub fn try_copy(bytes: &[u8]) -> MemoryResult<Self> {
        let mut copied = Self::new();
        copied.try_reserve_exact(bytes.len())?;
        copied.0.extend_from_slice(bytes);
        Ok(copied)
    }
}

/// An owned UTF-8 string with fallible construction and budgeted backing storage.
#[derive(Debug, Default)]
pub struct MetadataString(MetadataBytes);

impl MetadataString {
    /// Copy `value` through the shared Kernel allocator.
    pub fn try_copy(value: &str) -> MemoryResult<Self> {
        Ok(Self(MetadataBytes::try_copy(value.as_bytes())?))
    }

    /// Borrow the string without allocating.
    pub fn as_str(&self) -> &str {
        // SAFETY: Construction copies a str, and no API exposes mutable backing bytes.
        unsafe { std::str::from_utf8_unchecked(&self.0) }
    }

    /// Return the requested byte capacity retained by this string.
    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    /// Serialize JSON directly into the string's fallible UTF-8 storage.
    pub fn try_serialize<T: Serialize + ?Sized>(value: &T) -> MemoryResult<Self> {
        // serde_json emits UTF-8, and write_json returns no buffer on an incomplete write.
        Ok(Self(try_to_json(value)?))
    }
}

/// Serialize an existing value directly into a budgeted output buffer.
///
/// An output allocation failure returns `AllocationFailed`, including on the first write. Source
/// values, serializer internals, and error construction are outside this output-buffer accounting.
pub fn try_to_json<T: Serialize + ?Sized>(value: &T) -> MemoryResult<MetadataBytes> {
    let mut bytes = MetadataBytes::new();
    write_json(&mut bytes, value)?;
    Ok(bytes)
}

pub(crate) fn write_json<T: Serialize + ?Sized>(
    bytes: &mut MetadataBytes,
    value: &T,
) -> MemoryResult<()> {
    let mut writer = JsonWriter {
        bytes,
        failure: None,
    };
    match serde_json::to_writer(&mut writer, value) {
        Ok(()) => Ok(()),
        Err(_) => Err(writer.failure.unwrap_or(MemoryError::Serialization)),
    }
}

struct JsonWriter<'a> {
    bytes: &'a mut MetadataBytes,
    failure: Option<MemoryError>,
}

impl Write for JsonWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes.try_extend_from_slice(bytes).map_err(|error| {
            self.failure = Some(error);
            io::Error::from(io::ErrorKind::OutOfMemory)
        })?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl AsRef<[u8]> for MetadataBytes {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl Serialize for MetadataString {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}
impl PartialEq for MetadataString {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}
impl Eq for MetadataString {}
impl std::fmt::Display for MetadataString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// An owning iterator retaining charged capacity until it is dropped.
#[derive(Debug)]
pub struct IntoIter<T> {
    values: MetadataVec<T>,
    front: usize,
    back: usize,
}

impl<T> Iterator for IntoIter<T> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        if self.front == self.back {
            return None;
        }
        let index = self.front;
        self.front += 1;
        // SAFETY: Only [front, back) is initialized. The cursor excludes this element before
        // transferring ownership, and the backing vector has length zero to prevent double drop.
        Some(unsafe { self.values.0.as_ptr().add(index).read() })
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.back - self.front;
        (len, Some(len))
    }
}
impl<T> ExactSizeIterator for IntoIter<T> {}
impl<T> std::iter::FusedIterator for IntoIter<T> {}
impl<T> DoubleEndedIterator for IntoIter<T> {
    fn next_back(&mut self) -> Option<T> {
        if self.front == self.back {
            return None;
        }
        self.back -= 1;
        // SAFETY: The decremented cursor identifies one initialized, unconsumed element.
        Some(unsafe { self.values.0.as_ptr().add(self.back).read() })
    }
}
impl<T> Drop for IntoIter<T> {
    fn drop(&mut self) {
        // SAFETY: Only the remaining range is initialized. Slice drop also handles unwinding
        // while dropping an element; the zero-length vector then frees the backing allocation.
        unsafe {
            let remaining = std::ptr::slice_from_raw_parts_mut(
                self.values.0.as_mut_ptr().add(self.front),
                self.back - self.front,
            );
            std::ptr::drop_in_place(remaining);
        }
    }
}
impl<T> IntoIterator for MetadataVec<T> {
    type Item = T;
    type IntoIter = IntoIter<T>;
    fn into_iter(mut self) -> Self::IntoIter {
        let back = self.len();
        // SAFETY: The iterator becomes the owner of the initialized range and drops its elements.
        unsafe { self.0.set_len(0) };
        IntoIter {
            values: self,
            front: 0,
            back,
        }
    }
}

impl<'de, T: serde::Deserialize<'de>> serde::Deserialize<'de> for MetadataVec<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Items<T>(std::marker::PhantomData<T>);
        impl<'de, T: serde::Deserialize<'de>> serde::de::Visitor<'de> for Items<T> {
            type Value = MetadataVec<T>;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a metadata sequence")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut input: A,
            ) -> Result<Self::Value, A::Error> {
                let mut values = MetadataVec::new();
                while let Some(value) = input.next_element()? {
                    values.try_push(value).map_err(|error| {
                        super::record_failure(error);
                        serde::de::Error::custom(error)
                    })?;
                }
                Ok(values)
            }
        }
        deserializer.deserialize_seq(Items(std::marker::PhantomData))
    }
}

impl<T> Default for MetadataVec<T> {
    fn default() -> Self {
        Self::new()
    }
}
impl<T> From<Vec<T>> for MetadataVec<T> {
    fn from(values: Vec<T>) -> Self {
        values.into_iter().collect()
    }
}
impl<T> FromIterator<T> for MetadataVec<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut values = AllocVec::new_in(ContainerAllocator);
        values.extend(iter);
        Self(values)
    }
}
impl<T: Clone> Clone for MetadataVec<T> {
    fn clone(&self) -> Self {
        self.iter().cloned().collect()
    }
}
impl<T: PartialEq> PartialEq for MetadataVec<T> {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}
impl<T: Eq> Eq for MetadataVec<T> {}
impl<T> DerefMut for MetadataVec<T> {
    fn deref_mut(&mut self) -> &mut [T] {
        &mut self.0
    }
}
impl<T> MetadataVec<T> {
    /// Remove an element by index, shifting later elements. The index must exist.
    pub fn remove(&mut self, index: usize) -> T {
        self.0.remove(index)
    }
    /// Keep selected elements without changing allocated capacity.
    pub fn retain(&mut self, predicate: impl FnMut(&T) -> bool) {
        self.0.retain(predicate);
    }
    /// Remove an initial range without changing allocated capacity.
    pub fn remove_prefix(&mut self, count: usize) {
        let mut index = 0;
        self.0.retain(|_| {
            index += 1;
            index > count
        });
    }
}
impl<'a, T> IntoIterator for &'a MetadataVec<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
impl<'a, T> IntoIterator for &'a mut MetadataVec<T> {
    type Item = &'a mut T;
    type IntoIter = std::slice::IterMut<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

impl From<&str> for MetadataString {
    fn from(value: &str) -> Self {
        Self(value.as_bytes().iter().copied().collect())
    }
}
