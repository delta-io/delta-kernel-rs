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
use std::sync::Arc;

use delta_kernel_derive::internal_api;
use derive_more::Constructor;
use tracing::{debug, warn};

use crate::engine_data::GetData;
use crate::log_replay::deduplicator::{Deduplicator, FileActionInfo};
use crate::{EngineData, KernelResult, Result};

pub(crate) mod deduplicator;

/// The subset of file action fields that uniquely identifies it in the log, used for deduplication
/// of adds and removes during log replay.
#[derive(Debug, Hash, Eq, PartialEq, serde::Serialize, serde::Deserialize, Clone)]
pub struct FileActionKey {
    path: String,
    dv_unique_id: Option<String>,
}

impl FileActionKey {
    /// Build a key from a path and optional deletion-vector unique id.
    pub fn new(path: impl Into<String>, dv_unique_id: Option<String>) -> Self {
        let path = path.into();
        Self { path, dv_unique_id }
    }

    /// The data file path, as stored on the action.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Deletion-vector unique id, or `None` if the action carries no deletion vector. Same
    /// `path` with different `dv_unique_id` (e.g. an `add(P, dv=new) + remove(P, dv=old)`
    /// DV-replacement pair) refers to distinct logical file identities.
    pub fn dv_unique_id(&self) -> Option<&str> {
        self.dv_unique_id.as_deref()
    }
}

/// Maintains state and provides functionality for deduplicating file actions during log replay.
///
/// This struct is embedded in visitors to track which files have been seen across multiple
/// log batches. Since logs are processed newest-to-oldest, this deduplicator ensures that each
/// unique file (identified by path and deletion vector ID) is processed only once. Performing
/// deduplication at the visitor level avoids having to load all actions into memory at once,
/// significantly reducing memory usage for large Delta tables with extensive history.
///
/// TODO: Modify deduplication to track only file paths instead of (path, dv_unique_id).
/// More info here: https://github.com/delta-io/delta-kernel-rs/issues/701
#[derive(Constructor)]
pub(crate) struct FileActionDeduplicator<'seen> {
    /// A set of (data file path, dv_unique_id) pairs that have been seen thus
    /// far in the log for deduplication. This is a mutable reference to the set
    /// of seen file keys that persists across multiple log batches.
    seen_file_keys: &'seen mut HashSet<FileActionKey>,
    // TODO: Consider renaming to `is_commit_batch`, `deduplicate_batch`, or `save_batch`
    // to better reflect its role in deduplication logic.
    /// Whether we're processing a commit log JSON file (`true`) or a checkpoint file (`false`).
    /// When `true`, file actions are added to `seen_file_keys` as they're processed.
    is_log_batch: bool,
    /// Index of the getter containing the add.path column
    add_path_index: usize,
    /// Index of the getter containing the add.size column
    add_size_index: usize,
    /// Index of the getter containing the remove.path column
    remove_path_index: usize,
    /// Starting index for add action deletion vector columns
    add_dv_start_index: usize,
    /// Starting index for remove action deletion vector columns
    remove_dv_start_index: usize,
}

impl Deduplicator for FileActionDeduplicator<'_> {
    /// Checks if log replay already processed this logical file (in which case the current action
    /// should be ignored). If not already seen, register it so we can recognize future duplicates.
    /// Returns `true` if we have seen the file and should ignore it, `false` if we have not seen it
    /// and should process it.
    fn check_and_record_seen(&mut self, key: FileActionKey) -> bool {
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

    /// Extracts a file action key and determines if it's an add operation.
    /// This method examines the data at the given index using the provided getters
    /// to identify whether a file action exists and what type it is.
    ///
    /// # Parameters
    /// - `i`: Index position in the data structure to examine
    /// - `getters`: Collection of data getter implementations used to access the data
    /// - `skip_removes`: Whether to skip remove actions when extracting file actions
    ///
    /// # Returns
    /// - `Ok(Some(FileActionInfo))`: When a file action is found, This contains the key whether
    ///   it's an add operation, and the size of the file
    /// - `Ok(None)`: When no file action is found
    /// - `Err(...)`: On any error during extraction
    fn extract_file_action<'a>(
        &self,
        i: usize,
        getters: &[&'a dyn GetData<'a>],
        skip_removes: bool,
    ) -> KernelResult<Option<FileActionInfo>> {
        // Try to extract an add action by the required path column
        if let Some(path) = getters[self.add_path_index].get_str(i, "add.path")? {
            let size = match getters[self.add_size_index].get_long(i, "add.size")? {
                Some(s) => u64::try_from(s).unwrap_or_else(|e| {
                    warn!("Could not convert add.size {s} to u64: {e}");
                    0
                }),
                None => {
                    warn!("Add action without required size field");
                    0
                }
            };
            let dv_unique_id = self.extract_dv_unique_id(i, getters, self.add_dv_start_index)?;
            return Ok(Some(FileActionInfo {
                key: FileActionKey::new(path, dv_unique_id),
                size,
                is_add: true,
            }));
        }

        // The AddRemoveDedupVisitor skips remove actions when extracting file actions from a
        // checkpoint batch.
        if skip_removes {
            return Ok(None);
        }

        // Try to extract a remove action by the required path column
        if let Some(path) = getters[self.remove_path_index].get_str(i, "remove.path")? {
            let dv_unique_id = self.extract_dv_unique_id(i, getters, self.remove_dv_start_index)?;
            return Ok(Some(FileActionInfo {
                key: FileActionKey::new(path, dv_unique_id),
                size: 0,
                is_add: false,
            }));
        }

        // No file action found
        Ok(None)
    }

    /// Returns whether we are currently processing a log batch.
    ///
    /// `true` indicates we are processing a batch from a commit file.
    /// `false` indicates we are processing a batch from a checkpoint.
    fn is_log_batch(&self) -> bool {
        self.is_log_batch
    }
}

#[internal_api]
#[derive(Constructor)]
pub(crate) struct ActionsBatch {
    /// The batch of actions to be processed: each row is an action from the log.
    pub actions: Box<dyn EngineData>,
    /// Whether the batch is from a commit log (=true) or a checkpoint/CRC/elsewhere (=false).
    pub is_log_batch: bool,
}

impl ActionsBatch {
    /// HACK: a duplication of the pub(crate) field `actions` to allow us to export as
    /// 'internal-api' and let inspect-table example use it.
    #[allow(unused)]
    #[internal_api]
    pub(crate) fn actions(&self) -> &dyn EngineData {
        self.actions.as_ref()
    }
}

#[internal_api]
pub(crate) trait ParallelLogReplayProcessor {
    type Output;
    fn process_actions_batch_parallel(&self, actions_batch: ActionsBatch) -> Result<Self::Output>;
}

impl<T> ParallelLogReplayProcessor for Arc<T>
where
    T: ParallelLogReplayProcessor,
{
    type Output = T::Output;

    fn process_actions_batch_parallel(&self, actions_batch: ActionsBatch) -> Result<Self::Output> {
        T::process_actions_batch_parallel(self, actions_batch)
    }
}

/// This trait is used to determine if a processor's output contains any selected rows.
/// This is used to filter out batches with no selected rows from the log replay results.
#[internal_api]
pub(crate) trait HasSelectionVector {
    /// Check if the selection vector contains at least one selected row
    fn has_selected_rows(&self) -> bool;
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use rstest::rstest;

    use super::deduplicator::CheckpointDeduplicator;
    use super::*;
    use crate::engine_data::GetData;
    use crate::Result;

    /// Mock GetData implementation for testing
    struct MockGetData {
        string_values: HashMap<(usize, String), String>,
        int_values: HashMap<(usize, String), i32>,
        long_values: HashMap<(usize, String), i64>,
        errors: HashMap<(usize, String), String>,
    }

    impl MockGetData {
        fn new() -> Self {
            Self {
                string_values: HashMap::new(),
                int_values: HashMap::new(),
                long_values: HashMap::new(),
                errors: HashMap::new(),
            }
        }

        fn add_string(&mut self, row: usize, field: &str, value: &str) {
            self.string_values
                .insert((row, field.to_string()), value.to_string());
        }

        fn add_int(&mut self, row: usize, field: &str, value: i32) {
            self.int_values.insert((row, field.to_string()), value);
        }

        fn add_long(&mut self, row: usize, field: &str, value: i64) {
            self.long_values.insert((row, field.to_string()), value);
        }
    }

    impl<'a> GetData<'a> for MockGetData {
        fn get_str(&'a self, row_index: usize, field_name: &str) -> Result<Option<&'a str>> {
            if let Some(error_msg) = self.errors.get(&(row_index, field_name.to_string())) {
                return Err(crate::KernelError::Generic(error_msg.clone()));
            }
            Ok(self
                .string_values
                .get(&(row_index, field_name.to_string()))
                .map(|s| s.as_str()))
        }

        fn get_int(&'a self, row_index: usize, field_name: &str) -> Result<Option<i32>> {
            if let Some(error_msg) = self.errors.get(&(row_index, field_name.to_string())) {
                return Err(crate::KernelError::Generic(error_msg.clone()));
            }
            Ok(self
                .int_values
                .get(&(row_index, field_name.to_string()))
                .cloned())
        }

        fn get_long(&'a self, row_index: usize, field_name: &str) -> Result<Option<i64>> {
            if let Some(error_msg) = self.errors.get(&(row_index, field_name.to_string())) {
                return Err(crate::KernelError::Generic(error_msg.clone()));
            }
            Ok(self
                .long_values
                .get(&(row_index, field_name.to_string()))
                .cloned())
        }
    }

    /// Helper to create a FileActionDeduplicator with standard indices
    fn create_deduplicator(
        seen: &mut HashSet<FileActionKey>,
        is_log_batch: bool,
    ) -> FileActionDeduplicator<'_> {
        FileActionDeduplicator::new(
            seen,
            is_log_batch,
            0, // add_path_index
            1, // add_size_index,
            5, // remove_path_index
            2, // add_dv_start_index
            6, // remove_dv_start_index
        )
    }

    /// Helper to create a getters array with mocks at specific positions
    fn create_getters_with_mocks<'a>(
        add_mock: Option<&'a MockGetData>,
        remove_mock: Option<&'a MockGetData>,
    ) -> Vec<&'a dyn GetData<'a>> {
        use std::sync::LazyLock;
        static EMPTY: LazyLock<MockGetData> = LazyLock::new(MockGetData::new);

        let empty_ref = &*EMPTY;
        vec![
            add_mock.unwrap_or(empty_ref),    // 0: add.path
            add_mock.unwrap_or(empty_ref),    // 1: add.size
            add_mock.unwrap_or(empty_ref),    // 2: add.dv.storageType
            add_mock.unwrap_or(empty_ref),    // 3: add.dv.pathOrInlineDv
            add_mock.unwrap_or(empty_ref),    // 4: add.dv.offset
            remove_mock.unwrap_or(empty_ref), // 5: remove.path
            remove_mock.unwrap_or(empty_ref), // 6: remove.dv.storageType
            remove_mock.unwrap_or(empty_ref), // 7: remove.dv.pathOrInlineDv
            remove_mock.unwrap_or(empty_ref), // 8: remove.dv.offset
        ]
    }

    #[rstest]
    #[case::valid_size(Some(100), 100)]
    #[case::missing_size_defaults_to_zero(None, 0)]
    #[case::negative_size_defaults_to_zero(Some(-1), 0)]
    fn test_extract_file_action_add(
        #[case] raw_size: Option<i64>,
        #[case] expected_size: u64,
    ) -> Result<()> {
        let mut seen = HashSet::new();
        let deduplicator = create_deduplicator(&mut seen, true);

        let mut mock_add = MockGetData::new();
        mock_add.add_string(0, "add.path", "file1.parquet");
        if let Some(s) = raw_size {
            mock_add.add_long(0, "add.size", s);
        }
        let getters = create_getters_with_mocks(Some(&mock_add), None);
        let result = deduplicator.extract_file_action(0, &getters, false)?;

        let FileActionInfo { key, size, is_add } = result.unwrap();
        assert_eq!(key.path, "file1.parquet");
        assert!(key.dv_unique_id.is_none());
        assert!(is_add);
        assert_eq!(size, expected_size);

        Ok(())
    }

    #[test]
    fn test_extract_file_action_remove() -> Result<()> {
        let mut seen = HashSet::new();
        let deduplicator = create_deduplicator(&mut seen, true);

        let mut mock_remove = MockGetData::new();
        mock_remove.add_string(0, "remove.path", "file2.parquet");
        let getters = create_getters_with_mocks(None, Some(&mock_remove));
        let result = deduplicator.extract_file_action(0, &getters, false)?;

        assert!(result.is_some());
        let FileActionInfo { key, is_add, .. } = result.unwrap();
        assert_eq!(key.path, "file2.parquet");
        assert!(!is_add);

        Ok(())
    }

    #[test]
    fn test_extract_file_action_with_deletion_vector() -> Result<()> {
        let mut seen = HashSet::new();
        let deduplicator = create_deduplicator(&mut seen, true);

        let mut mock_dv = MockGetData::new();
        mock_dv.add_string(0, "add.path", "file_with_dv.parquet");
        mock_dv.add_string(0, "deletionVector.storageType", "s3");
        mock_dv.add_string(0, "deletionVector.pathOrInlineDv", "path/to/dv");
        mock_dv.add_int(0, "deletionVector.offset", 100);
        let getters = create_getters_with_mocks(Some(&mock_dv), None);
        let result = deduplicator.extract_file_action(0, &getters, false)?;

        assert!(result.is_some());
        let FileActionInfo { key, is_add, .. } = result.unwrap();
        assert!(matches!(
            key.dv_unique_id.as_deref(),
            Some("s3path/to/dv@100")
        ));
        assert!(is_add);

        Ok(())
    }

    #[test]
    fn test_extract_file_action_skip_removes() -> Result<()> {
        let mut seen = HashSet::new();
        let deduplicator = create_deduplicator(&mut seen, true);

        let mut mock_remove = MockGetData::new();
        mock_remove.add_string(0, "remove.path", "file2.parquet");
        let getters = create_getters_with_mocks(None, Some(&mock_remove));

        // With skip_removes=true, should return None
        assert!(deduplicator
            .extract_file_action(0, &getters, true)?
            .is_none());

        // With skip_removes=false, should return Some
        assert!(deduplicator
            .extract_file_action(0, &getters, false)?
            .is_some());

        Ok(())
    }

    #[test]
    fn test_extract_file_action_no_action_found() -> Result<()> {
        let mut seen = HashSet::new();
        let deduplicator = create_deduplicator(&mut seen, true);

        let getters = create_getters_with_mocks(None, None);
        assert!(deduplicator
            .extract_file_action(0, &getters, false)?
            .is_none());

        Ok(())
    }

    #[test]
    fn test_check_and_record_seen() {
        let mut seen = HashSet::new();

        // Pre-populate with an existing key
        let pre_existing_key = FileActionKey::new("existing.parquet", None);
        seen.insert(pre_existing_key.clone());

        let key1 = FileActionKey::new("file1.parquet", None);
        let key2 = FileActionKey::new("file2.parquet", None);
        let key_with_dv = FileActionKey::new("file1.parquet", Some("dv1".to_string()));

        // Test with log batch (should record keys)
        {
            let mut deduplicator = create_deduplicator(&mut seen, true);

            // Pre-existing key should be detected as duplicate
            assert!(deduplicator.check_and_record_seen(pre_existing_key.clone()));

            // First time seeing keys, should return false and record them
            assert!(!deduplicator.check_and_record_seen(key1.clone()));
            assert!(!deduplicator.check_and_record_seen(key2.clone()));
            assert!(!deduplicator.check_and_record_seen(key_with_dv.clone()));

            // Second time seeing keys, should return true (duplicates)
            assert!(deduplicator.check_and_record_seen(key1.clone()));
            assert!(deduplicator.check_and_record_seen(key_with_dv.clone()));
        }

        // Keys should be recorded in seen set
        assert!(seen.contains(&key1));
        assert!(seen.contains(&key2));
        assert!(seen.contains(&key_with_dv));

        // Test with checkpoint batch (should NOT record keys)
        {
            let mut deduplicator = create_deduplicator(&mut seen, false);

            let new_key = FileActionKey::new("new.parquet", None);

            // First time seeing new_key in checkpoint, should return false but NOT record it
            assert!(!deduplicator.check_and_record_seen(new_key.clone()));
            // Still returns false on second call (not recorded)
            assert!(!deduplicator.check_and_record_seen(new_key.clone()));

            // Existing keys from seen set should still be detected
            assert!(deduplicator.check_and_record_seen(key1.clone()));
        }
    }

    #[test]
    fn test_is_log_batch() {
        let mut seen = HashSet::new();

        // Test with is_log_batch = true
        let deduplicator_log = create_deduplicator(&mut seen, true);
        assert!(deduplicator_log.is_log_batch());

        // Test with is_log_batch = false
        let deduplicator_checkpoint = create_deduplicator(&mut seen, false);
        assert!(!deduplicator_checkpoint.is_log_batch());
    }

    // ==================== CheckpointDeduplicator Tests ====================

    #[test]
    fn test_checkpoint_extract_file_action_add() -> Result<()> {
        let seen = HashSet::new();
        let deduplicator = CheckpointDeduplicator::try_new(&seen, 0, 2, 3)?;

        let mut mock_add = MockGetData::new();
        mock_add.add_string(0, "add.path", "checkpoint_file.parquet");
        let getters = create_getters_with_mocks(Some(&mock_add), None);
        let result = deduplicator.extract_file_action(0, &getters, false)?;

        assert!(result.is_some());
        let FileActionInfo { key, is_add, .. } = result.unwrap();
        assert_eq!(key.path, "checkpoint_file.parquet");
        assert!(key.dv_unique_id.is_none());
        assert!(is_add);

        Ok(())
    }

    #[test]
    fn test_checkpoint_extract_file_action_with_deletion_vector() -> Result<()> {
        let seen = HashSet::new();
        let deduplicator = CheckpointDeduplicator::try_new(&seen, 0, 1, 2)?;

        let mut mock_dv = MockGetData::new();
        mock_dv.add_string(0, "add.path", "file_with_dv.parquet");
        mock_dv.add_string(0, "deletionVector.storageType", "s3");
        mock_dv.add_string(0, "deletionVector.pathOrInlineDv", "path/to/dv");
        mock_dv.add_int(0, "deletionVector.offset", 100);
        let getters = create_getters_with_mocks(Some(&mock_dv), None);
        let result = deduplicator.extract_file_action(0, &getters, false)?;

        assert!(result.is_some());
        let FileActionInfo { key, is_add, .. } = result.unwrap();
        assert_eq!(key.path, "file_with_dv.parquet");
        assert!(matches!(
            key.dv_unique_id.as_deref(),
            Some("s3path/to/dv@100")
        ));
        assert!(is_add);

        Ok(())
    }

    #[test]
    fn test_checkpoint_deduplicator_filters_commit_duplicates() -> Result<()> {
        let mut seen = HashSet::new();

        // Files "seen" during commit processing
        seen.insert(FileActionKey::new("modified_in_commit.parquet", None));
        seen.insert(FileActionKey::new(
            "modified_with_dv.parquet",
            Some("dv123".to_string()),
        ));

        let mut deduplicator = CheckpointDeduplicator::try_new(&seen, 0, 2, 3)?;

        // File modified in commit - should be filtered from checkpoint
        let commit_modified = FileActionKey::new("modified_in_commit.parquet", None);
        assert!(
            deduplicator.check_and_record_seen(commit_modified),
            "Files seen in commits should be filtered from checkpoint"
        );

        // File with DV modified in commit - should be filtered
        let commit_modified_dv =
            FileActionKey::new("modified_with_dv.parquet", Some("dv123".to_string()));
        assert!(
            deduplicator.check_and_record_seen(commit_modified_dv),
            "Files with DVs seen in commits should be filtered from checkpoint"
        );

        // File only in checkpoint - should NOT be filtered
        let checkpoint_only = FileActionKey::new("checkpoint_only.parquet", None);
        assert!(
            !deduplicator.check_and_record_seen(checkpoint_only),
            "Files only in checkpoint should not be filtered"
        );

        Ok(())
    }
}
