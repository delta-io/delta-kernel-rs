use std::io::{self, Write};
use std::ops::{Deref, DerefMut};

use serde::Serialize;

use super::{LimitedAllocator, MemoryBudget, MemoryError, MemoryResult};

/// An allocator-aware vector exposing only fallible growth.
///
/// The backing allocation is charged by capacity. Allocations owned by `T` must independently
/// use the same budget; putting an ordinary `String` here does not charge its contents.
#[derive(Debug)]
pub struct LimitedVec<T>(Vec<T, LimitedAllocator>);

impl<T> LimitedVec<T> {
    /// Create an empty vector without allocating payload storage.
    pub fn new(budget: &MemoryBudget) -> Self {
        Self::new_in(budget.allocator())
    }

    /// Create an empty vector retaining an explicit standard allocator.
    pub fn new_in(allocator: LimitedAllocator) -> Self {
        Self(Vec::new_in(allocator))
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

    /// Return the shared budget retained by this vector's allocator.
    pub fn budget(&self) -> &MemoryBudget {
        self.0.allocator().budget()
    }

    pub(crate) fn into_vec(self) -> Vec<T, LimitedAllocator> {
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
        MemoryError::AllocationFailed {
            limit: self.0.allocator().limit(),
        }
    }
}

impl<T> Deref for LimitedVec<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// An owned byte buffer whose capacity is charged to a shared budget.
pub type LimitedBytes = LimitedVec<u8>;

impl LimitedBytes {
    /// Copy bytes into budgeted storage without mutating the buffer if growth fails.
    pub fn try_extend_from_slice(&mut self, bytes: &[u8]) -> MemoryResult<()> {
        self.try_reserve(bytes.len())?;
        self.0.extend_from_slice(bytes);
        Ok(())
    }

    /// Copy a slice into a buffer with exactly requested initial capacity.
    pub fn try_copy(budget: &MemoryBudget, bytes: &[u8]) -> MemoryResult<Self> {
        let mut copied = Self::new(budget);
        copied.try_reserve_exact(bytes.len())?;
        copied.0.extend_from_slice(bytes);
        Ok(copied)
    }
}

/// An owned UTF-8 string with fallible construction and budgeted backing storage.
#[derive(Debug)]
pub struct LimitedString(LimitedBytes);

impl LimitedString {
    /// Copy `value`, charging its bytes to `budget`.
    pub fn try_copy(budget: &MemoryBudget, value: &str) -> MemoryResult<Self> {
        Ok(Self(LimitedBytes::try_copy(budget, value.as_bytes())?))
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
    pub fn try_serialize<T: Serialize + ?Sized>(
        budget: &MemoryBudget,
        value: &T,
    ) -> MemoryResult<Self> {
        // serde_json emits UTF-8, and write_json returns no buffer on an incomplete write.
        Ok(Self(try_to_json(budget, value)?))
    }
}

/// Serialize an existing value directly into a budgeted output buffer.
///
/// An output allocation failure returns `AllocationFailed`, including on the first write. Source
/// values, serializer internals, and error construction are outside this output-buffer budget.
pub fn try_to_json<T: Serialize + ?Sized>(
    budget: &MemoryBudget,
    value: &T,
) -> MemoryResult<LimitedBytes> {
    let mut bytes = LimitedBytes::new(budget);
    write_json(&mut bytes, value)?;
    Ok(bytes)
}

pub(crate) fn write_json<T: Serialize + ?Sized>(
    bytes: &mut LimitedBytes,
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
    bytes: &'a mut LimitedBytes,
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

impl AsRef<[u8]> for LimitedBytes {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl Serialize for LimitedString {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}
impl PartialEq for LimitedString {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}
impl Eq for LimitedString {}
impl std::fmt::Display for LimitedString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// An owning iterator retaining charged capacity until it is dropped.
#[derive(Debug)]
pub struct LimitedIntoIter<T> {
    values: LimitedVec<T>,
    front: usize,
    back: usize,
}

impl<T> Iterator for LimitedIntoIter<T> {
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
impl<T> ExactSizeIterator for LimitedIntoIter<T> {}
impl<T> std::iter::FusedIterator for LimitedIntoIter<T> {}
impl<T> DoubleEndedIterator for LimitedIntoIter<T> {
    fn next_back(&mut self) -> Option<T> {
        if self.front == self.back {
            return None;
        }
        self.back -= 1;
        // SAFETY: The decremented cursor identifies one initialized, unconsumed element.
        Some(unsafe { self.values.0.as_ptr().add(self.back).read() })
    }
}
impl<T> Drop for LimitedIntoIter<T> {
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
impl<T> IntoIterator for LimitedVec<T> {
    type Item = T;
    type IntoIter = LimitedIntoIter<T>;
    fn into_iter(mut self) -> Self::IntoIter {
        let back = self.len();
        // SAFETY: The iterator becomes the owner of the initialized range and drops its elements.
        unsafe { self.0.set_len(0) };
        LimitedIntoIter {
            values: self,
            front: 0,
            back,
        }
    }
}

impl<'de, T: serde::Deserialize<'de>> serde::Deserialize<'de> for LimitedVec<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Items<T>(std::marker::PhantomData<T>);
        impl<'de, T: serde::Deserialize<'de>> serde::de::Visitor<'de> for Items<T> {
            type Value = LimitedVec<T>;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a metadata sequence")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut input: A,
            ) -> Result<Self::Value, A::Error> {
                let budget =
                    MemoryBudget::current().unwrap_or_else(|| MemoryBudget::new(usize::MAX));
                let mut values = LimitedVec::new(&budget);
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

/// Retained sequence with fallible growth under the active allocation budget.
///
/// Infallible conversion from Vec, collection, and Clone preserve ordinary allocation semantics
/// outside the cap. Elements' owned allocations need independent accounting.
#[derive(Debug)]
pub struct RetainedVec<T>(RetainedStorage<T>);

#[derive(Debug)]
enum RetainedStorage<T> {
    Global(Vec<T>),
    Limited(LimitedVec<T>),
}

impl<T> RetainedVec<T> {
    /// Create empty storage using the active or global budget, if configured.
    pub fn new() -> Self {
        match MemoryBudget::current() {
            Some(budget) => Self(RetainedStorage::Limited(LimitedVec::new(&budget))),
            None => Self(RetainedStorage::Global(Vec::new())),
        }
    }
    /// Append an element with fallible capacity growth.
    pub fn try_push(&mut self, value: T) -> MemoryResult<()> {
        match &mut self.0 {
            RetainedStorage::Limited(values) => values.try_push(value),
            RetainedStorage::Global(values) => {
                values
                    .try_reserve(1)
                    .map_err(|_| MemoryError::AllocationFailed { limit: usize::MAX })?;
                values.push(value);
                Ok(())
            }
        }
    }
    /// Append elements, retaining successfully appended values if a later growth fails.
    pub fn try_extend(&mut self, values: impl IntoIterator<Item = T>) -> MemoryResult<()> {
        for value in values {
            self.try_push(value)?;
        }
        Ok(())
    }
    /// Return the retained element capacity.
    pub fn capacity(&self) -> usize {
        match &self.0 {
            RetainedStorage::Global(v) => v.capacity(),
            RetainedStorage::Limited(v) => v.capacity(),
        }
    }
    /// Drop all elements, retaining charged capacity.
    pub fn clear(&mut self) {
        match &mut self.0 {
            RetainedStorage::Global(v) => v.clear(),
            RetainedStorage::Limited(v) => v.clear(),
        }
    }
    /// Remove the last element, retaining charged capacity.
    pub fn pop(&mut self) -> Option<T> {
        match &mut self.0 {
            RetainedStorage::Global(v) => v.pop(),
            RetainedStorage::Limited(v) => v.pop(),
        }
    }
    /// Remove an element by index, shifting later elements. The index must exist.
    pub fn remove(&mut self, index: usize) -> T {
        match &mut self.0 {
            RetainedStorage::Global(v) => v.remove(index),
            RetainedStorage::Limited(v) => v.0.remove(index),
        }
    }
    /// Keep only the elements selected by the predicate, retaining charged capacity.
    pub fn retain(&mut self, predicate: impl FnMut(&T) -> bool) {
        match &mut self.0 {
            RetainedStorage::Global(v) => v.retain(predicate),
            RetainedStorage::Limited(v) => v.0.retain(predicate),
        }
    }
    /// Remove the first `count` elements, retaining charged capacity.
    pub fn remove_prefix(&mut self, count: usize) {
        let mut index = 0;
        self.retain(|_| {
            index += 1;
            index > count
        });
    }
}
impl<T> Default for RetainedVec<T> {
    fn default() -> Self {
        Self::new()
    }
}
impl<T> From<Vec<T>> for RetainedVec<T> {
    fn from(value: Vec<T>) -> Self {
        Self(RetainedStorage::Global(value))
    }
}
impl<T> FromIterator<T> for RetainedVec<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        iter.into_iter().collect::<Vec<_>>().into()
    }
}
impl<T: Clone> Clone for RetainedVec<T> {
    fn clone(&self) -> Self {
        self.iter().cloned().collect()
    }
}
impl<T: PartialEq> PartialEq for RetainedVec<T> {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}
impl<T: Eq> Eq for RetainedVec<T> {}
impl<T> Deref for RetainedVec<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        match &self.0 {
            RetainedStorage::Global(v) => v,
            RetainedStorage::Limited(v) => v,
        }
    }
}
impl<T> DerefMut for RetainedVec<T> {
    fn deref_mut(&mut self) -> &mut [T] {
        match &mut self.0 {
            RetainedStorage::Global(v) => v,
            RetainedStorage::Limited(v) => v.as_mut_slice(),
        }
    }
}
impl<T> IntoIterator for RetainedVec<T> {
    type Item = T;
    type IntoIter = itertools::Either<std::vec::IntoIter<T>, LimitedIntoIter<T>>;
    fn into_iter(self) -> Self::IntoIter {
        match self.0 {
            RetainedStorage::Global(v) => itertools::Either::Left(v.into_iter()),
            RetainedStorage::Limited(v) => itertools::Either::Right(v.into_iter()),
        }
    }
}
impl<'a, T> IntoIterator for &'a RetainedVec<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
impl<'a, T> IntoIterator for &'a mut RetainedVec<T> {
    type Item = &'a mut T;
    type IntoIter = std::slice::IterMut<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}
