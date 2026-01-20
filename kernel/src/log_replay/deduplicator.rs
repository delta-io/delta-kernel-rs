//! Deduplication abstraction for log replay processors.
//!
//! The [`Deduplicator`] trait supports two deduplication strategies:
//!
//! - **JSON commit files** (`is_log_batch = true`): Tracks (path, dv_unique_id) and updates
//!   the hashmap as files are seen. Implementation: [`FileActionDeduplicator`]
//!
//! - **Checkpoint files** (`is_log_batch = false`): Uses (path, dv_unique_id) to filter actions
//!   using a read-only hashmap pre-populated from the commit log phase. Future implementation.
//!
//! [`FileActionDeduplicator`]: crate::log_replay::FileActionDeduplicator

use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::engine_data::{GetData, TypedGetData};
use crate::log_replay::FileActionKey;
use crate::DeltaResult;

pub(crate) trait Deduplicator {
    /// Extracts a file action key from the data. Returns `(key, is_add)` if found.
    ///
    /// TODO: Remove the skip_removes field in the future. The caller is responsible for using the
    /// correct Deduplicator instance depeding on whether the batch belongs to a commit or to a
    /// checkpoint.
    fn extract_file_action<'a>(
        &self,
        i: usize,
        getters: &[&'a dyn GetData<'a>],
        skip_removes: bool,
    ) -> DeltaResult<Option<(FileActionKey, bool)>>;

    /// Checks if this file has been seen. When `is_log_batch() = true`, updates the hashmap
    /// to track new files. Returns `true` if the file should be filtered out.
    fn check_and_record_seen(&mut self, key: FileActionKey) -> bool;

    /// Returns `true` for commit log batches (updates hashmap), `false` for checkpoints (read-only).
    fn is_log_batch(&self) -> bool;

    /// Extracts the deletion vector unique ID if it exists.
    ///
    /// This function retrieves the necessary fields for constructing a deletion vector unique ID
    /// by accessing `getters` at `dv_start_index` and the following two indices. Specifically:
    /// - `dv_start_index` retrieves the storage type (`deletionVector.storageType`).
    /// - `dv_start_index + 1` retrieves the path or inline deletion vector (`deletionVector.pathOrInlineDv`).
    /// - `dv_start_index + 2` retrieves the optional offset (`deletionVector.offset`).
    fn extract_dv_unique_id<'a>(
        &self,
        i: usize,
        getters: &[&'a dyn GetData<'a>],
        dv_start_index: usize,
    ) -> DeltaResult<Option<String>> {
        let Some(storage_type) =
            getters[dv_start_index].get_opt(i, "deletionVector.storageType")?
        else {
            return Ok(None);
        };
        let path_or_inline = getters[dv_start_index + 1].get(i, "deletionVector.pathOrInlineDv")?;
        let offset = getters[dv_start_index + 2].get_opt(i, "deletionVector.offset")?;

        Ok(Some(DeletionVectorDescriptor::unique_id_from_parts(
            storage_type,
            path_or_inline,
            offset,
        )))
    }
}
