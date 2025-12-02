//! Deduplication abstraction for log replay processors.
//!
//! The [`Deduplicator`] trait supports two deduplication strategies:
//!
//! - **JSON commit files** (`is_log_batch = true`): Tracks (path, dv_unique_id) and updates
//!   the hashmap as files are seen. Implementation: [`FileActionDeduplicator`]
//!
//! - **Checkpoint files** (`is_log_batch = false`): Uses path-only keys with a read-only hashmap
//!   pre-populated from the commit log phase. Future implementation.
//!
//! [`FileActionDeduplicator`]: crate::log_replay::FileActionDeduplicator

use crate::{engine_data::GetData, DeltaResult};

pub(crate) trait Deduplicator {
    /// Key type for identifying file actions. JSON deduplicators use `FileActionKey`
    /// (path + dv_unique_id), checkpoint deduplicators may use path-only keys.
    type Key;

    /// Extracts a file action key from the data. Returns `(key, is_add)` if found.
    fn extract_file_action<'a>(
        &self,
        i: usize,
        getters: &[&'a dyn GetData<'a>],
        skip_removes: bool,
    ) -> DeltaResult<Option<(Self::Key, bool)>>;

    /// Checks if this file has been seen. When `is_log_batch() = true`, updates the hashmap
    /// to track new files. Returns `true` if the file should be filtered out.
    fn check_and_record_seen(&mut self, key: Self::Key) -> bool;

    /// Returns `true` for commit log batches (updates hashmap), `false` for checkpoints (read-only).
    fn is_log_batch(&self) -> bool;
}
