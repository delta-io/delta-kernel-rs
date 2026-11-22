use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;

use serde::ser::SerializeSeq;

use super::{LimitedAllocator, LimitedString, LimitedVec, MemoryBudget, MemoryError, MemoryResult};

/// Fallible file-action deduplication state keyed by path and optional DV identity.
///
/// `true` from [`Self::try_insert`] identifies the first occurrence in a newest-first replay.
/// Repeated keys allocate nothing. This is storage for already-normalized keys, not a substitute
/// for Kernel's deletion-vector identity construction or action-selection rules.
#[derive(Debug)]
pub struct FileKeySet {
    buckets: LimitedVec<Option<FileKey>>,
    len: usize,
    hasher: RandomState,
}

#[derive(Debug)]
struct FileKey {
    path: LimitedString,
    dv: Option<LimitedString>,
}

impl FileKeySet {
    /// Create an empty set using a shared requested-byte budget.
    pub fn new(budget: &MemoryBudget) -> Self {
        Self::new_in(budget.allocator())
    }

    /// Create an empty set retaining an explicit standard allocator.
    pub fn new_in(allocator: LimitedAllocator) -> Self {
        Self {
            buckets: LimitedVec::new_in(allocator),
            len: 0,
            hasher: RandomState::new(),
        }
    }

    /// Record an owned key, leaving existing keys intact if an allocation fails.
    pub fn try_insert(&mut self, path: &str, dv: Option<&str>) -> MemoryResult<bool> {
        if self.contains(path, dv) {
            return Ok(false);
        }
        let key = FileKey {
            path: LimitedString::try_copy(self.buckets.budget(), path)?,
            dv: dv
                .map(|dv| LimitedString::try_copy(self.buckets.budget(), dv))
                .transpose()?,
        };
        if self.len >= self.buckets.len() / 2 {
            self.try_grow()?;
        }
        insert_key(&mut self.buckets, &self.hasher, key);
        self.len += 1;
        Ok(true)
    }

    /// Look up a normalized key without allocating.
    pub fn contains(&self, path: &str, dv: Option<&str>) -> bool {
        if self.buckets.is_empty() {
            return false;
        }
        let mut index = hash_key(&self.hasher, path, dv) & (self.buckets.len() - 1);
        loop {
            match &self.buckets[index] {
                Some(key)
                    if key.path.as_str() == path
                        && key.dv.as_ref().map(LimitedString::as_str) == dv =>
                {
                    return true;
                }
                Some(_) => index = (index + 1) & (self.buckets.len() - 1),
                None => return false,
            }
        }
    }

    /// Return the number of unique keys retained.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Return whether no keys have been retained.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn try_grow(&mut self) -> MemoryResult<()> {
        let capacity = self
            .buckets
            .len()
            .max(4)
            .checked_mul(2)
            .ok_or(MemoryError::CapacityOverflow)?;
        let mut buckets = LimitedVec::new(self.buckets.budget());
        buckets.try_reserve_exact(capacity)?;
        for _ in 0..capacity {
            buckets.try_push(None)?;
        }
        // Rehash only after every fallible operation succeeds. Old and new buckets coexist and
        // remain charged; the key payloads move without cloning.
        for key in self
            .buckets
            .as_mut_slice()
            .iter_mut()
            .filter_map(Option::take)
        {
            insert_key(&mut buckets, &self.hasher, key);
        }
        self.buckets = buckets;
        Ok(())
    }
}

fn hash_key(hasher: &RandomState, path: &str, dv: Option<&str>) -> usize {
    hasher.hash_one((path, dv)) as usize
}

fn insert_key(buckets: &mut LimitedVec<Option<FileKey>>, hasher: &RandomState, key: FileKey) {
    let mut index = hash_key(
        hasher,
        key.path.as_str(),
        key.dv.as_ref().map(LimitedString::as_str),
    ) & (buckets.len() - 1);
    while buckets[index].is_some() {
        index = (index + 1) & (buckets.len() - 1);
    }
    buckets.as_mut_slice()[index] = Some(key);
}

impl serde::Serialize for FileKeySet {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(serde::Serialize)]
        struct Key<'a> {
            path: &'a str,
            dv_unique_id: Option<&'a str>,
        }
        let mut sequence = serializer.serialize_seq(Some(self.len))?;
        for key in self.buckets.iter().flatten() {
            sequence.serialize_element(&Key {
                path: key.path.as_str(),
                dv_unique_id: key.dv.as_ref().map(LimitedString::as_str),
            })?;
        }
        sequence.end()
    }
}
impl<'de> serde::Deserialize<'de> for FileKeySet {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Keys;
        impl<'de> serde::de::Visitor<'de> for Keys {
            type Value = FileKeySet;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("replay keys")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut input: A,
            ) -> Result<Self::Value, A::Error> {
                let budget =
                    MemoryBudget::current().unwrap_or_else(|| MemoryBudget::new(usize::MAX));
                let mut keys = FileKeySet::new(&budget);
                while let Some(key) = input.next_element::<crate::log_replay::FileActionKey>()? {
                    keys.try_insert(key.path(), key.dv_unique_id())
                        .map_err(|error| {
                            super::record_failure(error);
                            serde::de::Error::custom(error)
                        })?;
                }
                Ok(keys)
            }
        }
        deserializer.deserialize_seq(Keys)
    }
}
