use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::iter::{FusedIterator, Map};
use std::slice;

use indexmap::IndexMap;
use itertools::Either;

use super::{LimitedIntoIter, LimitedVec, MemoryBudget, MemoryError, MemoryResult};

/// Ordered string-keyed storage with fallible entry and hash-index growth.
///
/// Keys and value-owned payloads need independent accounting. Infallible `FromIterator`
/// construction retains ordinary IndexMap storage outside the cap.
pub struct OrderedMap<V>(Storage<V>);

impl<V: std::fmt::Debug> std::fmt::Debug for OrderedMap<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

#[derive(Debug)]
enum Storage<V> {
    Global(IndexMap<String, V>),
    Limited {
        entries: LimitedVec<(String, V)>,
        buckets: LimitedVec<usize>,
        hasher: RandomState,
    },
}

/// Borrowed ordered values, retaining double-ended iteration.
pub(crate) type Values<'a, V> = Either<
    indexmap::map::Values<'a, String, V>,
    Map<slice::Iter<'a, (String, V)>, fn(&(String, V)) -> &V>,
>;
/// Consuming ordered values, retaining ownership of allocated capacity.
pub(crate) type IntoValues<V> = Either<
    indexmap::map::IntoValues<String, V>,
    Map<LimitedIntoIter<(String, V)>, fn((String, V)) -> V>,
>;

impl<V> OrderedMap<V> {
    pub(crate) fn new() -> Self {
        match MemoryBudget::current() {
            Some(budget) => Self(Storage::Limited {
                entries: LimitedVec::new(&budget),
                buckets: LimitedVec::new(&budget),
                hasher: RandomState::new(),
            }),
            None => Self(Storage::Global(IndexMap::new())),
        }
    }

    pub(crate) fn try_insert(&mut self, key: String, value: V) -> MemoryResult<Option<V>> {
        if let Some(index) = self.get_index_of(&key) {
            return Ok(match &mut self.0 {
                Storage::Global(map) => map.insert(key, value),
                Storage::Limited { entries, .. } => Some(std::mem::replace(
                    &mut entries.as_mut_slice()[index].1,
                    value,
                )),
            });
        }
        match &mut self.0 {
            Storage::Global(map) => {
                map.try_reserve(1)
                    .map_err(|_| MemoryError::AllocationFailed { limit: usize::MAX })?;
                Ok(map.insert(key, value))
            }
            Storage::Limited {
                entries,
                buckets,
                hasher,
            } => {
                entries.try_reserve(1)?;
                if entries.len() >= buckets.len() / 2 {
                    let len = buckets
                        .len()
                        .max(4)
                        .checked_mul(2)
                        .ok_or(MemoryError::CapacityOverflow)?;
                    let mut grown = LimitedVec::new(buckets.budget());
                    grown.try_reserve_exact(len)?;
                    for _ in 0..len {
                        grown.try_push(0)?;
                    }
                    for (index, (key, _)) in entries.iter().enumerate() {
                        insert_index(grown.as_mut_slice(), hasher.hash_one(key) as usize, index);
                    }
                    *buckets = grown;
                }
                let index = entries.len();
                let hash = hasher.hash_one(&key) as usize;
                entries.try_push((key, value))?;
                insert_index(buckets.as_mut_slice(), hash, index);
                Ok(None)
            }
        }
    }

    /// Return the number of entries.
    pub fn len(&self) -> usize {
        match &self.0 {
            Storage::Global(map) => map.len(),
            Storage::Limited { entries, .. } => entries.len(),
        }
    }
    /// Return whether there are no entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Look up the insertion index for a key.
    pub fn get_index_of(&self, key: &str) -> Option<usize> {
        match &self.0 {
            Storage::Global(map) => map.get_index_of(key),
            Storage::Limited {
                entries,
                buckets,
                hasher,
            } => {
                if buckets.is_empty() {
                    return None;
                }
                let mut slot = hasher.hash_one(key) as usize & (buckets.len() - 1);
                loop {
                    let index = buckets[slot].checked_sub(1)?;
                    if entries[index].0 == key {
                        return Some(index);
                    }
                    slot = (slot + 1) & (buckets.len() - 1);
                }
            }
        }
    }
    /// Return whether the key exists.
    pub fn contains_key(&self, key: &str) -> bool {
        self.get_index_of(key).is_some()
    }
    /// Borrow the value for a key.
    pub fn get(&self, key: &str) -> Option<&V> {
        self.get_full(key).map(|(_, _, value)| value)
    }
    /// Borrow an entry by key, with its insertion index.
    pub fn get_full(&self, key: &str) -> Option<(usize, &String, &V)> {
        let index = self.get_index_of(key)?;
        self.get_index(index)
            .map(|(key, value)| (index, key, value))
    }
    /// Borrow an entry by insertion index.
    pub fn get_index(&self, index: usize) -> Option<(&String, &V)> {
        match &self.0 {
            Storage::Global(map) => map.get_index(index),
            Storage::Limited { entries, .. } => entries.get(index).map(|(key, value)| (key, value)),
        }
    }
    /// Iterate entries in insertion order.
    pub fn iter(
        &self,
    ) -> impl ExactSizeIterator<Item = (&String, &V)> + DoubleEndedIterator + FusedIterator {
        match &self.0 {
            Storage::Global(map) => Either::Left(map.iter()),
            Storage::Limited { entries, .. } => {
                Either::Right(entries.iter().map(|(key, value)| (key, value)))
            }
        }
    }
    /// Iterate values in insertion order.
    pub fn values(&self) -> Values<'_, V> {
        match &self.0 {
            Storage::Global(map) => Either::Left(map.values()),
            Storage::Limited { entries, .. } => {
                Either::Right(entries.iter().map(|(_, value)| value))
            }
        }
    }
    /// Mutate values without permitting infallible map growth.
    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut V> {
        match &mut self.0 {
            Storage::Global(map) => Either::Left(map.values_mut()),
            Storage::Limited { entries, .. } => {
                Either::Right(entries.as_mut_slice().iter_mut().map(|(_, value)| value))
            }
        }
    }
    /// Iterate keys in insertion order.
    pub fn keys(&self) -> impl ExactSizeIterator<Item = &String> {
        self.iter().map(|(key, _)| key)
    }
    /// Consume the map in insertion order.
    pub fn into_values(self) -> IntoValues<V> {
        match self.0 {
            Storage::Global(map) => Either::Left(map.into_values()),
            Storage::Limited { entries, .. } => {
                Either::Right(entries.into_iter().map(|(_, value)| value))
            }
        }
    }
}

impl<V> From<IndexMap<String, V>> for OrderedMap<V> {
    fn from(map: IndexMap<String, V>) -> Self {
        Self(Storage::Global(map))
    }
}
impl<V> FromIterator<(String, V)> for OrderedMap<V> {
    fn from_iter<T: IntoIterator<Item = (String, V)>>(iter: T) -> Self {
        Self(Storage::Global(iter.into_iter().collect()))
    }
}
impl<V: PartialEq> PartialEq for OrderedMap<V> {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len()
            && self
                .iter()
                .all(|(key, value)| other.get(key) == Some(value))
    }
}
impl<V: Eq> Eq for OrderedMap<V> {}

#[cfg(test)]
impl<V> std::ops::Index<&str> for OrderedMap<V> {
    type Output = V;
    fn index(&self, key: &str) -> &V {
        self.get(key).unwrap()
    }
}

fn insert_index(buckets: &mut [usize], hash: usize, index: usize) {
    let mut slot = hash & (buckets.len() - 1);
    while buckets[slot] != 0 {
        slot = (slot + 1) & (buckets.len() - 1);
    }
    buckets[slot] = index + 1;
}
