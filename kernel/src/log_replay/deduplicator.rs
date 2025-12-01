//! Deduplication abstraction for log replay processors.
//!
//! The [`Deduplicator`] trait supports two deduplication strategies:
//!
//! - **JSON commit files** (`is_log_batch = true`): Tracks (path, dv_unique_id) and updates
//!   the hashmap as files are seen. Implementation: [`FileActionDeduplicator`]
//!
//! - **Checkpoint files** (`is_log_batch = false`): Uses a read-only Tracks (path, dv_unique_id)
//!   hashmap pre-populated from the commit log phase. Future implementation.
//!
//! [`FileActionDeduplicator`]: crate::log_replay::FileActionDeduplicator

use crate::{engine_data::GetData, log_replay::FileActionKey, DeltaResult};

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

#[allow(unused)]
pub(crate) struct CheckpointDeduplicator<'a> {
    seen_file_keys: &'a HashSet<FileActionKey>,
    add_path_index: usize,
}
impl CheckpointDeduplicator<'_> {
    #[allow(unused)]
    pub(crate) fn try_new<'a>(
        seen_file_keys: &'a HashSet<FileActionKey>,
        add_path_index: usize,
    ) -> DeltaResult<CheckpointDeduplicator<'a>> {
        Ok(CheckpointDeduplicator {
            seen_file_keys,
            add_path_index,
        })
    }
}

impl Deduplicator for CheckpointDeduplicator<'_> {
    type Key = String;

    fn extract_file_action<'a>(
        &self,
        i: usize,
        getters: &[&'a dyn GetData<'a>],
        _skip_removes: bool,
    ) -> DeltaResult<Option<(Self::Key, bool)>> {
        // Try to extract an add action by the required path column
        if let Some(path) = getters[self.add_path_index].get_str(i, "add.path")? {
            Ok(Some((path.to_string(), true)))
        } else {
            Ok(None)
        }
    }

    fn check_and_record_seen(&mut self, key: Self::Key) -> bool {
        self.seen_file_keys.contains(&key)
    }

    fn is_log_batch(&self) -> bool {
        false
    }
}
