use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::iter::{FusedIterator, Map};
use std::slice;

use indexmap::IndexMap;

use super::{IntoIter, MemoryError, MemoryResult, MetadataString, MetadataVec};

/// Ordered string-keyed storage with tracked keys, entries, and hash-index capacity.
pub struct OrderedMap<V> {
    entries: MetadataVec<(MetadataString, V)>,
    buckets: MetadataVec<usize>,
    hasher: RandomState,
}

impl<V: std::fmt::Debug> std::fmt::Debug for OrderedMap<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

pub(crate) type Values<'a, V> =
    Map<slice::Iter<'a, (MetadataString, V)>, fn(&(MetadataString, V)) -> &V>;
pub(crate) type IntoValues<V> = Map<IntoIter<(MetadataString, V)>, fn((MetadataString, V)) -> V>;

impl<V> OrderedMap<V> {
    pub(crate) fn new() -> Self {
        Self {
            entries: MetadataVec::new(),
            buckets: MetadataVec::new(),
            hasher: RandomState::new(),
        }
    }

    pub(crate) fn try_insert(&mut self, key: String, value: V) -> MemoryResult<Option<V>> {
        if let Some(index) = self.get_index_of(&key) {
            return Ok(Some(std::mem::replace(
                &mut self.entries.as_mut_slice()[index].1,
                value,
            )));
        }
        let key = MetadataString::try_copy(&key)?;
        self.entries.try_reserve(1)?;
        if self.entries.len() >= self.buckets.len() / 2 {
            let len = self
                .buckets
                .len()
                .max(4)
                .checked_mul(2)
                .ok_or(MemoryError::CapacityOverflow)?;
            let mut grown = MetadataVec::new();
            grown.try_reserve_exact(len)?;
            for _ in 0..len {
                grown.try_push(0)?;
            }
            for (index, (key, _)) in self.entries.iter().enumerate() {
                insert_index(
                    &mut grown,
                    self.hasher.hash_one(key.as_str()) as usize,
                    index,
                );
            }
            self.buckets = grown;
        }
        let index = self.entries.len();
        let hash = self.hasher.hash_one(key.as_str()) as usize;
        self.entries.try_push((key, value))?;
        insert_index(&mut self.buckets, hash, index);
        Ok(None)
    }

    /// Return the number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    /// Return whether there are no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    /// Look up the insertion index for a key.
    pub fn get_index_of(&self, key: &str) -> Option<usize> {
        if self.buckets.is_empty() {
            return None;
        }
        let mut slot = self.hasher.hash_one(key) as usize & (self.buckets.len() - 1);
        loop {
            let index = self.buckets[slot].checked_sub(1)?;
            if self.entries[index].0.as_str() == key {
                return Some(index);
            }
            slot = (slot + 1) & (self.buckets.len() - 1);
        }
    }
    /// Return whether a key exists.
    pub fn contains_key(&self, key: &str) -> bool {
        self.get_index_of(key).is_some()
    }
    /// Borrow the value for a key.
    pub fn get(&self, key: &str) -> Option<&V> {
        self.get_full(key).map(|(_, _, value)| value)
    }
    /// Borrow an entry and its insertion index.
    pub fn get_full(&self, key: &str) -> Option<(usize, &str, &V)> {
        let index = self.get_index_of(key)?;
        self.get_index(index)
            .map(|(key, value)| (index, key, value))
    }
    /// Borrow an entry by insertion index.
    pub fn get_index(&self, index: usize) -> Option<(&str, &V)> {
        self.entries
            .get(index)
            .map(|(key, value)| (key.as_str(), value))
    }
    /// Iterate entries in insertion order.
    pub fn iter(
        &self,
    ) -> impl ExactSizeIterator<Item = (&str, &V)> + DoubleEndedIterator + FusedIterator {
        self.entries
            .iter()
            .map(|(key, value)| (key.as_str(), value))
    }
    /// Iterate values in insertion order.
    pub fn values(&self) -> Values<'_, V> {
        self.entries.iter().map(|(_, value)| value)
    }
    /// Mutate values without allowing infallible map growth.
    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut V> {
        self.entries
            .as_mut_slice()
            .iter_mut()
            .map(|(_, value)| value)
    }
    /// Iterate keys in insertion order.
    pub fn keys(&self) -> impl ExactSizeIterator<Item = &str> {
        self.iter().map(|(key, _)| key)
    }
    /// Consume values in insertion order.
    pub fn into_values(self) -> IntoValues<V> {
        self.entries.into_iter().map(|(_, value)| value)
    }
}

impl<V> From<IndexMap<String, V>> for OrderedMap<V> {
    fn from(map: IndexMap<String, V>) -> Self {
        map.into_iter().collect()
    }
}
impl<V> FromIterator<(String, V)> for OrderedMap<V> {
    fn from_iter<T: IntoIterator<Item = (String, V)>>(iter: T) -> Self {
        let mut map = Self::new();
        for (key, value) in iter {
            if map.try_insert(key, value).is_err() {
                std::alloc::handle_alloc_error(std::alloc::Layout::new::<(MetadataString, V)>());
            }
        }
        map
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
