//! Deduplication abstraction for log replay processors.
//!
//! The [`Deduplicator`] trait supports two deduplication strategies:
//!
//! - **JSON commit files** (`is_log_batch = true`): Tracks (path, dv_unique_id) and updates
//!   the seen set as files are encountered. Implementation: [`FileActionDeduplicator`]
//!
//! - **Checkpoint files** (`is_log_batch = false`): Uses (path, dv_unique_id) to filter actions
//!   against a read-only seen set pre-populated from the commit log phase.
//!   Implementation: [`CheckpointDeduplicator`]
//!
//! [`FileActionDeduplicator`]: crate::log_replay::FileActionDeduplicator

use hashbrown::HashSet;

use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::engine_data::{GetData, TypedGetData};
use crate::log_replay::{FileActionKey, FileActionKeyRef};
use crate::DeltaResult;

/// A file action extracted from a batch row, carrying a borrowed path and an optional owned
/// deletion vector unique ID. Keeping the path borrowed avoids a heap allocation for files
/// that turn out to be duplicates and never need to be inserted into the seen set.
pub(crate) struct ExtractedFileAction<'a> {
    /// Borrowed path from the action batch column data.
    pub(crate) path: &'a str,
    /// Deletion vector unique ID, if present. Owned because it is built from a `format!`
    /// of three separate DV fields; there is no single column to borrow from.
    pub(crate) dv_unique_id: Option<String>,
    /// `true` for add actions, `false` for remove actions.
    pub(crate) is_add: bool,
}

pub(crate) trait Deduplicator {
    /// Extracts a file action from row `i` without allocating an owned key.
    ///
    /// Returns `Some(action)` if a file action is present at this row, `None` otherwise.
    /// The returned `action.path` borrows from `getters` with lifetime `'a`.
    ///
    /// TODO: Remove the `skip_removes` parameter in the future. The caller is responsible for
    /// using the correct [`Deduplicator`] instance depending on whether the batch belongs to a
    /// commit or to a checkpoint.
    fn extract_file_action<'a>(
        &self,
        i: usize,
        getters: &[&'a dyn GetData<'a>],
        skip_removes: bool,
    ) -> DeltaResult<Option<ExtractedFileAction<'a>>>;

    /// Checks if this file action has been seen before. When `is_log_batch() = true`, records
    /// new files so future duplicates can be recognized. Returns `true` if the file should be
    /// filtered out (already seen), `false` if it is new and should be processed.
    ///
    /// Only allocates an owned [`FileActionKey`] when the file is new and needs to be inserted
    /// into the seen set. Already-seen files are identified via a zero-copy borrowed-key lookup.
    fn check_and_record_seen(&mut self, action: ExtractedFileAction<'_>) -> bool;

    /// Returns `true` for commit log batches (updates the seen set), `false` for checkpoints
    /// (read-only).
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

/// Read-only deduplicator for checkpoint processing.
///
/// Unlike [`FileActionDeduplicator`] which mutably tracks files, this uses an immutable
/// reference to filter checkpoint actions against files already seen from commits.
/// Only handles add actions (no removes), and never modifies the seen set.
///
/// [`FileActionDeduplicator`]: crate::log_replay::FileActionDeduplicator
#[allow(unused)]
pub(crate) struct CheckpointDeduplicator<'a> {
    seen_file_keys: &'a HashSet<FileActionKey>,
    add_path_index: usize,
    add_dv_start_index: usize,
}

impl<'a> CheckpointDeduplicator<'a> {
    #[allow(unused)]
    pub(crate) fn try_new(
        seen_file_keys: &'a HashSet<FileActionKey>,
        add_path_index: usize,
        add_dv_start_index: usize,
    ) -> DeltaResult<Self> {
        Ok(CheckpointDeduplicator {
            seen_file_keys,
            add_path_index,
            add_dv_start_index,
        })
    }
}

impl Deduplicator for CheckpointDeduplicator<'_> {
    /// Extracts add action key only (checkpoints skip removes). `skip_removes` is ignored.
    fn extract_file_action<'b>(
        &self,
        i: usize,
        getters: &[&'b dyn GetData<'b>],
        _skip_removes: bool,
    ) -> DeltaResult<Option<ExtractedFileAction<'b>>> {
        let Some(path) = getters[self.add_path_index].get_str(i, "add.path")? else {
            return Ok(None);
        };
        let dv_unique_id = self.extract_dv_unique_id(i, getters, self.add_dv_start_index)?;
        Ok(Some(ExtractedFileAction {
            path,
            dv_unique_id,
            is_add: true,
        }))
    }

    /// Read-only check against seen set. Returns `true` if file should be filtered out.
    fn check_and_record_seen(&mut self, action: ExtractedFileAction<'_>) -> bool {
        self.seen_file_keys.contains(&FileActionKeyRef::new(
            action.path,
            action.dv_unique_id.as_deref(),
        ))
    }

    /// Always `false` - checkpoint batches never update the seen set.
    fn is_log_batch(&self) -> bool {
        false
    }
}
