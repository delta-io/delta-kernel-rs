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
use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::engine_data::{GetData, TypedGetData};
use crate::scan::data_skipping::DataSkippingFilter;
use crate::{DeltaResult, EngineData};

use delta_kernel_derive::internal_api;

use std::collections::HashSet;

use tracing::debug;

/// The subset of file action fields that uniquely identifies it in the log, used for deduplication
/// of adds and removes during log replay.
#[derive(Debug, Hash, Eq, PartialEq, Clone)]
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
///
/// TODO: Modify deduplication to track only file paths instead of (path, dv_unique_id).
/// More info here: https://github.com/delta-io/delta-kernel-rs/issues/701
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
    /// # Parameters
    /// - `i`: Index position in the data structure to examine
    /// - `getters`: Collection of data getter implementations used to access the data
    /// - `skip_removes`: Whether to skip remove actions when extracting file actions
    ///
    /// # Returns
    /// - `Ok(Some((key, is_add)))`: When a file action is found, returns the key and whether it's an add operation
    /// - `Ok(None)`: When no file action is found
    /// - `Err(...)`: On any error during extraction
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

#[internal_api]
pub(crate) struct ActionsBatch {
    /// The batch of actions to be processed: each row is an action from the log.
    pub actions: Box<dyn EngineData>,
    /// Whether the batch is from a commit log (=true) or a checkpoint/CRC/elsewhere (=false).
    pub is_log_batch: bool,
}

impl ActionsBatch {
    /// Creates a new `ActionsBatch` instance. See [`LogReplayProcessor::process_actions_batch`] for
    /// usage.
    ///
    /// # Parameters
    /// - `actions`: A boxed [`EngineData`] instance representing the actions batch.
    /// - `is_log_batch`: A boolean indicating whether the batch is from a commit log (`true`) or
    ///   a checkpoint/CRC/elsewhere (`false`).
    pub(crate) fn new(actions: Box<dyn EngineData>, is_log_batch: bool) -> Self {
        Self {
            actions,
            is_log_batch,
        }
    }

    /// HACK: a duplication of the pub(crate) field `actions` to allow us to export as
    /// 'internal-api' and let inspect-table example use it.
    #[allow(unused)]
    #[internal_api]
    pub(crate) fn actions(&self) -> &dyn EngineData {
        self.actions.as_ref()
    }
}

/// A trait for processing batches of actions from Delta transaction logs during log replay.
///
/// Log replay processors scan transaction logs in **reverse chronological order** (newest to oldest),
/// filtering and transforming action batches into specialized output types. These processors:
///
/// - **Track and deduplicate file actions** to apply appropriate `Remove` actions to corresponding
///   `Add` actions (and omit the file from the log replay output)
/// - **Maintain selection vectors** to indicate which actions in each batch should be included.
/// - **Apply custom filtering logic** based on the processor’s purpose (e.g., checkpointing, scanning).
/// - **Data skipping** filters are applied to the initial selection vector to reduce the number of rows
///   processed by the processor, (if a filter is provided).
///
/// # Implementations
///
/// - [`ScanLogReplayProcessor`]: Used for table scans, this processor filters and selects deduplicated
///   `Add` actions from log batches to reconstruct the view of the table at a specific point in time.
///   Note that scans do not expose `Remove` actions. Data skipping may be applied when a predicate is
///   provided.
///
/// - [`ActionReconciliationProcessor`]: Used for action reconciliation (including checkpoint writing),
///   this processor filters and selects actions from log batches for inclusion in V1 spec checkpoint files.
///   Unlike scans, action reconciliation processing includes additional actions, such as `Remove`, `Metadata`,
///   and `Protocol`, required to fully reconstruct table state. Data skipping is not applied during action
///   reconciliation processing.
///
/// [`ActionReconciliationProcessor`]: crate::action_reconciliation::log_replay::ActionReconciliationProcessor
///
/// # Action Iterator Input
///
/// The [`LogReplayProcessor::process_actions_iter`] method is the entry point for log replay processing.
/// It takes as input an iterator of (actions batch, is_commit_batch flag) tuples and returns an iterator of
/// processor-specific output types with selection vectors. The is_commit_batch bool flag in each tuple
/// indicates whether the batch came from a commit log (`true`) or checkpoint (`false`). Action batches
/// **must** be sorted by the order of the actions in the log from most recent to oldest.
///
/// Each row that is selected in the returned output **must** be included in the processor's result
/// (e.g., in scan results or checkpoint files), while non-selected rows **must** be ignored.
///
/// # Output Types
///
/// The [`LogReplayProcessor::Output`] type represents the material result of log replay, and it must
/// implement the [`HasSelectionVector`] trait to allow filtering of irrelevant rows:
///
/// - For **scans**, the output type is [`ScanMetadata`], which contains the file actions (`Add`
///   actions) that need to be applied to build the table's view, accompanied by a
///   **selection vector** that identifies which rows should be included. A transform vector may
///   also be included to handle schema changes, such as renaming columns or modifying data types.
///
/// - For **checkpoints**, the output type is [`FilteredEngineData`], which includes the actions
///   necessary to write to the checkpoint file (`Add`, `Remove`, `Metadata`, `Protocol` actions),
///   filtered by the **selection vector** to determine which rows are included in the final checkpoint.
///
/// TODO: Refactor the Change Data Feed (CDF) processor to use this trait.
pub(crate) trait LogReplayProcessor: Sized {
    /// The type of results produced by this processor must implement the
    /// [`HasSelectionVector`] trait to allow filtering out batches with no selected rows.
    type Output: HasSelectionVector;

    /// Processes a batch of actions and returns the filtered results.
    /// # Parameters
    /// - `actions_batch` - An [`ActionsBatch`] which includes a boxed [`EngineData`] instance
    ///   representing a batch of actions and a boolean flag indicating whether the batch originates
    ///   from a commit log, `false` if from a checkpoint.
    ///
    /// Returns a [`DeltaResult`] containing the processor’s output, which includes only selected actions.
    ///
    /// Note: Since log replay is stateful, processing may update internal processor state (e.g., deduplication sets).
    fn process_actions_batch(&mut self, actions_batch: ActionsBatch) -> DeltaResult<Self::Output>;

    /// Applies the processor to an actions iterator and filters out empty results.
    ///
    /// This method:
    /// 1. Applies `process_actions_batch` to each action batch
    /// 2. Maintains processor state across all batches
    /// 3. Automatically filters out batches with no selected rows
    ///
    /// # Parameters
    /// - `action_iter`: Iterator of [`ActionsBatch`], where each batch contains actions and the
    ///   boolean flag indicates whether the batch came from a commit log (`true`) or checkpoint
    ///   (`false`). Actions _must_ be provided in reverse chronological order.
    ///
    /// # Returns
    /// An iterator that yields the output type of the processor, containing only non-empty results
    /// (batches where at least one row was selected).
    fn process_actions_iter(
        mut self,
        action_iter: impl Iterator<Item = DeltaResult<ActionsBatch>>,
    ) -> impl Iterator<Item = DeltaResult<Self::Output>> {
        action_iter
            .map(move |actions_batch| self.process_actions_batch(actions_batch?))
            .filter(|res| {
                res.as_ref()
                    .ok()
                    .is_none_or(|result| result.has_selected_rows())
            })
    }

    /// Builds the initial selection vector for the action batch, used to filter out rows that
    /// are not relevant to the current processor's purpose (e.g., checkpointing, scanning).
    /// This method performs a first pass of filtering using an optional [`DataSkippingFilter`].
    /// If no filter is provided, it assumes that all rows should be selected.
    ///
    /// The selection vector is further updated based on the processor's logic in the
    /// `process_actions_batch` method.
    ///
    /// # Parameters
    /// - `batch`: A reference to the batch of actions to be processed.
    ///
    /// # Returns
    /// A `DeltaResult<Vec<bool>>`, where each boolean indicates if the corresponding row should be included.
    /// If no filter is provided, all rows are selected.
    fn build_selection_vector(&self, batch: &dyn EngineData) -> DeltaResult<Vec<bool>> {
        match self.data_skipping_filter() {
            Some(filter) => filter.apply(batch),
            None => Ok(vec![true; batch.len()]), // If no filter is provided, select all rows
        }
    }

    /// Returns an optional reference to the [`DataSkippingFilter`] used to filter rows
    /// when building the initial selection vector in `build_selection_vector`.
    /// If `None` is returned, no filter is applied, and all rows are selected.
    fn data_skipping_filter(&self) -> Option<&DataSkippingFilter>;
}

/// This trait is used to determine if a processor's output contains any selected rows.
/// This is used to filter out batches with no selected rows from the log replay results.
pub(crate) trait HasSelectionVector {
    /// Check if the selection vector contains at least one selected row
    fn has_selected_rows(&self) -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine_data::GetData;
    use crate::DeltaResult;
    use std::collections::{HashMap, HashSet};

    /// Mock GetData implementation for testing
    struct MockGetData {
        string_values: HashMap<(usize, String), String>,
        int_values: HashMap<(usize, String), i32>,
        errors: HashMap<(usize, String), String>,
    }

    impl MockGetData {
        fn new() -> Self {
            Self {
                string_values: HashMap::new(),
                int_values: HashMap::new(),
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
    }

    impl<'a> GetData<'a> for MockGetData {
        fn get_str(&'a self, row_index: usize, field_name: &str) -> DeltaResult<Option<&'a str>> {
            if let Some(error_msg) = self.errors.get(&(row_index, field_name.to_string())) {
                return Err(crate::Error::Generic(error_msg.clone()));
            }
            Ok(self
                .string_values
                .get(&(row_index, field_name.to_string()))
                .map(|s| s.as_str()))
        }

        fn get_int(&'a self, row_index: usize, field_name: &str) -> DeltaResult<Option<i32>> {
            if let Some(error_msg) = self.errors.get(&(row_index, field_name.to_string())) {
                return Err(crate::Error::Generic(error_msg.clone()));
            }
            Ok(self
                .int_values
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
            empty_ref,                        // 1: (unused)
            add_mock.unwrap_or(empty_ref),    // 2: add.dv.storageType
            add_mock.unwrap_or(empty_ref),    // 3: add.dv.pathOrInlineDv
            add_mock.unwrap_or(empty_ref),    // 4: add.dv.offset
            remove_mock.unwrap_or(empty_ref), // 5: remove.path
            remove_mock.unwrap_or(empty_ref), // 6: remove.dv.storageType
            remove_mock.unwrap_or(empty_ref), // 7: remove.dv.pathOrInlineDv
            remove_mock.unwrap_or(empty_ref), // 8: remove.dv.offset
        ]
    }

    #[test]
    fn test_extract_file_action_add() -> DeltaResult<()> {
        let mut seen = HashSet::new();
        let deduplicator = create_deduplicator(&mut seen, true);

        let mut mock_add = MockGetData::new();
        mock_add.add_string(0, "add.path", "file1.parquet");
        let getters = create_getters_with_mocks(Some(&mock_add), None);
        let result = deduplicator.extract_file_action(0, &getters, false)?;

        assert!(result.is_some());
        let (key, is_add) = result.unwrap();
        assert_eq!(key.path, "file1.parquet");
        assert!(key.dv_unique_id.is_none());
        assert!(is_add);

        Ok(())
    }

    #[test]
    fn test_extract_file_action_remove() -> DeltaResult<()> {
        let mut seen = HashSet::new();
        let deduplicator = create_deduplicator(&mut seen, true);

        let mut mock_remove = MockGetData::new();
        mock_remove.add_string(0, "remove.path", "file2.parquet");
        let getters = create_getters_with_mocks(None, Some(&mock_remove));
        let result = deduplicator.extract_file_action(0, &getters, false)?;

        assert!(result.is_some());
        let (key, is_add) = result.unwrap();
        assert_eq!(key.path, "file2.parquet");
        assert!(!is_add);

        Ok(())
    }

    #[test]
    fn test_extract_file_action_with_deletion_vector() -> DeltaResult<()> {
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
        let (key, is_add) = result.unwrap();
        assert!(matches!(
            key.dv_unique_id.as_deref(),
            Some("s3path/to/dv@100")
        ));
        assert!(is_add);

        Ok(())
    }

    #[test]
    fn test_extract_file_action_skip_removes() -> DeltaResult<()> {
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
    fn test_extract_file_action_no_action_found() -> DeltaResult<()> {
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
}
