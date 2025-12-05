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

use std::collections::HashSet;

use crate::{
    actions::deletion_vector::DeletionVectorDescriptor,
    engine_data::{GetData, TypedGetData as _},
    log_replay::FileActionKey,
    DeltaResult,
};

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
    add_dv_start_index: usize,
}
impl CheckpointDeduplicator<'_> {
    #[allow(unused)]
    pub(crate) fn try_new<'a>(
        seen_file_keys: &'a HashSet<FileActionKey>,
        add_path_index: usize,
        add_dv_start_index: usize,
    ) -> DeltaResult<CheckpointDeduplicator<'a>> {
        Ok(CheckpointDeduplicator {
            seen_file_keys,
            add_path_index,
            add_dv_start_index,
        })
    }
}

impl Deduplicator for CheckpointDeduplicator<'_> {
    type Key = FileActionKey;

    fn extract_file_action<'a>(
        &self,
        i: usize,
        getters: &[&'a dyn GetData<'a>],
        _skip_removes: bool,
    ) -> DeltaResult<Option<(Self::Key, bool)>> {
        // Try to extract an add action by the required path column
        if let Some(path) = getters[self.add_path_index].get_str(i, "add.path")? {
            let dv_unique_id = extract_dv_unique_id(i, getters, self.add_dv_start_index)?;
            Ok(Some((FileActionKey::new(path, dv_unique_id), true)))
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

/// Extracts the deletion vector unique ID if it exists.
///
/// This function retrieves the necessary fields for constructing a deletion vector unique ID
/// by accessing `getters` at `dv_start_index` and the following two indices. Specifically:
/// - `dv_start_index` retrieves the storage type (`deletionVector.storageType`).
/// - `dv_start_index + 1` retrieves the path or inline deletion vector (`deletionVector.pathOrInlineDv`).
/// - `dv_start_index + 2` retrieves the optional offset (`deletionVector.offset`).
pub(crate) fn extract_dv_unique_id<'a>(
    i: usize,
    getters: &[&'a dyn GetData<'a>],
    dv_start_index: usize,
) -> DeltaResult<Option<String>> {
    match getters[dv_start_index].get_opt(i, "deletionVector.storageType")? {
        Some(storage_type) => {
            let path_or_inline =
                getters[dv_start_index + 1].get(i, "deletionVector.pathOrInlineDv")?;
            let offset = getters[dv_start_index + 2].get_opt(i, "deletionVector.offset")?;

            Ok(Some(DeletionVectorDescriptor::unique_id_from_parts(
                storage_type,
                path_or_inline,
                offset,
            )))
        }
        None => Ok(None),
    }
}
