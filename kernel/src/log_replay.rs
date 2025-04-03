//! This module provides log replay utilities.
//!
//! Log replay is the process of transforming an iterator of action batches (read from Delta
//! transaction logs) into an iterator of filtered/transformed actions for specific use cases.
//! The logs, which record all table changes as JSON entries, are processed batch by batch,
//! typically from newest to oldest.
//!
//! Log replay is currently implemented for table scans, which filter and apply transformations
//! to produce file actions which builds the view of the table state at a specific point in time.  
//! Future extensions will support additional log replay processors beyond the current use case.
//! (e.g. checkpointing: filter actions to include only those needed to rebuild table state)
//!
//! This module provides structures for efficient batch processing, focusing on file action
//! deduplication with `FileActionDeduplicator` which tracks unique files across log batches
//! to minimize memory usage for tables with extensive history.

use std::collections::HashSet;

use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::engine_data::{GetData, TypedGetData};
use crate::DeltaResult;

use tracing::debug;

/// The subset of file action fields that uniquely identifies it in the log, used for deduplication
/// of adds and removes during log replay.
#[derive(Debug, Hash, Eq, PartialEq)]
pub(crate) struct FileActionKey {
    pub(crate) path: String,
    pub(crate) dv_unique_id: Option<String>,
}
impl FileActionKey {
    pub(crate) fn new(path: impl Into<String>, dv_unique_id: Option<String>) -> Self {
        let path = path.into();
        Self { path, dv_unique_id }
    }
}

/// Maintains state and provides functionality for deduplicating file actions during log replay.
///
/// This struct is embedded in visitors to track which files have been seen across multiple
/// log batches. Since logs are processed newest-to-oldest, this deduplicator ensures that each
/// unique file (identified by path and deletion vector ID) is processed only once. Performing
/// deduplication at the visitor level avoids having to load all actions into memory at once,
/// significantly reducing memory usage for large Delta tables with extensive history.
pub(crate) struct FileActionDeduplicator<'seen> {
    /// A set of (data file path, dv_unique_id) pairs that have been seen thus
    /// far in the log for deduplication. This is a mutable reference to the set
    /// of seen file keys that persists across multiple log batches.
    seen_file_keys: &'seen mut HashSet<FileActionKey>,
    // TODO: Consider renaming to `is_commit_batch`, `deduplicate_batch`, or `save_batch`
    // to better reflect its role in deduplication logic.
    /// Whether we're processing a log batch (as opposed to a checkpoint)
    is_log_batch: bool,
    /// Index of the getter containing the add.path column
    add_path_index: usize,
    /// Index of the getter containing the remove.path column
    remove_path_index: usize,
    /// Starting index for add action deletion vector columns
    add_dv_start_index: usize,
    /// Starting index for remove action deletion vector columns
    remove_dv_start_index: usize,
}

impl<'seen> FileActionDeduplicator<'seen> {
    pub(crate) fn new(
        seen_file_keys: &'seen mut HashSet<FileActionKey>,
        is_log_batch: bool,
        add_path_index: usize,
        remove_path_index: usize,
        add_dv_start_index: usize,
        remove_dv_start_index: usize,
    ) -> Self {
        Self {
            seen_file_keys,
            is_log_batch,
            add_path_index,
            remove_path_index,
            add_dv_start_index,
            remove_dv_start_index,
        }
    }

    /// Checks if log replay already processed this logical file (in which case the current action
    /// should be ignored). If not already seen, register it so we can recognize future duplicates.
    /// Returns `true` if we have seen the file and should ignore it, `false` if we have not seen it
    /// and should process it.
    pub(crate) fn check_and_record_seen(&mut self, key: FileActionKey) -> bool {
        // Note: each (add.path + add.dv_unique_id()) pair has a
        // unique Add + Remove pair in the log. For example:
        // https://github.com/delta-io/delta/blob/master/spark/src/test/resources/delta/table-with-dv-large/_delta_log/00000000000000000001.json

        if self.seen_file_keys.contains(&key) {
            debug!(
                "Ignoring duplicate ({}, {:?}) in scan, is log {}",
                key.path, key.dv_unique_id, self.is_log_batch
            );
            true
        } else {
            debug!(
                "Including ({}, {:?}) in scan, is log {}",
                key.path, key.dv_unique_id, self.is_log_batch
            );
            if self.is_log_batch {
                // Remember file actions from this batch so we can ignore duplicates as we process
                // batches from older commit and/or checkpoint files. We don't track checkpoint
                // batches because they are already the oldest actions and never replace anything.
                self.seen_file_keys.insert(key);
            }
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
    fn extract_dv_unique_id<'a>(
        &self,
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

    /// Extracts a file action key and determines if it's an add operation.
    /// This method examines the data at the given index using the provided getters
    /// to identify whether a file action exists and what type it is.
    ///
    /// # Arguments
    ///
    /// * `i` - Index position in the data structure to examine
    /// * `getters` - Collection of data getter implementations used to access the data
    /// * `skip_removes` - Whether to skip remove actions when extracting file actions
    ///
    /// # Returns
    ///
    /// * `Ok(Some((key, is_add)))` - When a file action is found, returns the key and whether it's an add operation
    /// * `Ok(None)` - When no file action is found
    /// * `Err(...)` - On any error during extraction
    pub(crate) fn extract_file_action<'a>(
        &self,
        i: usize,
        getters: &[&'a dyn GetData<'a>],
        skip_removes: bool,
    ) -> DeltaResult<Option<(FileActionKey, bool)>> {
        // Try to extract an add action by the required path column
        if let Some(path) = getters[self.add_path_index].get_str(i, "add.path")? {
            let dv_unique_id = self.extract_dv_unique_id(i, getters, self.add_dv_start_index)?;
            return Ok(Some((FileActionKey::new(path, dv_unique_id), true)));
        }

        // The AddRemoveDedupVisitor skips remove actions when extracting file actions from a checkpoint batch.
        if skip_removes {
            return Ok(None);
        }

        // Try to extract a remove action by the required path column
        if let Some(path) = getters[self.remove_path_index].get_str(i, "remove.path")? {
            let dv_unique_id = self.extract_dv_unique_id(i, getters, self.remove_dv_start_index)?;
            return Ok(Some((FileActionKey::new(path, dv_unique_id), false)));
        }

        // No file action found
        Ok(None)
    }

    /// Returns whether we are currently processing a log batch.
    ///
    /// `true` indicates we are processing a batch from a commit file.
    /// `false` indicates we are processing a batch from a checkpoint.
    pub(crate) fn is_log_batch(&self) -> bool {
        self.is_log_batch
    }
}
