//! The [`CheckpointLogReplayProcessor`] implements specialized log replay logic for creating
//! checkpoint files. It processes log files in reverse chronological order (newest to oldest)
//! and selects the set of actions to include in a checkpoint for a specific version.
//!
//! ## Actions Included for Checkpointing
//!
//! For checkpoint creation, this processor applies several filtering and deduplication
//! steps to each batch of log actions:
//!
//! 1. **Protocol and Metadata**: Retains exactly one of each - keeping only the latest protocol
//!    and metadata actions.
//! 2. **Txn Actions**: Keeps exactly one `txn` action for each unique app ID, always selecting
//!    the latest one encountered.
//! 3. **File Actions**: Resolves file actions to produce the latest state of the table, keeping
//!    the most recent valid add actions and unexpired remove actions (tombstones) that are newer
//!    than `minimum_file_retention_timestamp`.
//!
//! ## Architecture
//!
//! - [`CheckpointVisitor`]: Implements [`RowVisitor`] to examine each action in a batch and
//!   determine if it should be included in the checkpoint. It maintains state for deduplication
//!   across multiple actions in a batch and efficiently handles all filtering rules.
//!
//! - [`CheckpointLogReplayProcessor`]: Implements the [`LogReplayProcessor`] trait and orchestrates
//!   the overall process. For each batch of log actions, it:
//!   1. Creates a visitor with the current deduplication state
//!   2. Applies the visitor to filter actions in the batch
//!   3. Tracks state for deduplication across batches
//!   4. Produces a [`CheckpointBatch`] result which includes both the filtered data and counts of
//!      actions selected for the checkpoint file
//!
//! [`CheckpointMetadata`]: crate::actions::CheckpointMetadata
use crate::engine_data::{FilteredEngineData, GetData, RowVisitor, TypedGetData as _};
use crate::log_replay::{
    FileActionDeduplicator, FileActionKey, HasSelectionVector, LogReplayProcessor,
};
use crate::scan::data_skipping::DataSkippingFilter;
use crate::schema::{column_name, ColumnName, ColumnNamesAndTypes, DataType};
use crate::utils::require;
use crate::{DeltaResult, EngineData, Error};

use std::collections::HashSet;
use std::sync::LazyLock;

/// The [`CheckpointLogReplayProcessor`] is an implementation of the [`LogReplayProcessor`]
/// trait that filters log segment actions for inclusion in a V1 spec checkpoint file. This
/// processor is leveraged when creating a single-file V2 checkpoint as the V2 spec schema is
/// a superset of the V1 spec schema, with the addition of a [`CheckpointMetadata`] action.
pub(crate) struct CheckpointLogReplayProcessor {
    /// Tracks file actions that have been seen during log replay to avoid duplicates.
    /// Contains (data file path, dv_unique_id) pairs as `FileActionKey` instances.
    seen_file_keys: HashSet<FileActionKey>,
    /// Indicates whether a protocol action has been seen in the log.
    seen_protocol: bool,
    /// Indicates whether a metadata action has been seen in the log.
    seen_metadata: bool,
    /// Set of transaction app IDs that have been processed to avoid duplicates.
    seen_txns: HashSet<String>,
    /// Minimum timestamp for file retention, used for filtering expired tombstones.
    minimum_file_retention_timestamp: i64,
}

/// This struct is the output of the [`CheckpointLogReplayProcessor`].
///
/// It contains the filtered batch of actions to be included in the checkpoint,
/// along with statistics about the number of actions filtered for inclusion.
pub(crate) struct CheckpointBatch {
    /// The filtered batch of actions to be included in the checkpoint.
    pub(crate) filtered_data: FilteredEngineData,
    /// The number of actions in the batch filtered for inclusion in the checkpoint.
    pub(crate) actions_count: i64,
    /// The number of add actions in the batch filtered for inclusion in the checkpoint.
    pub(crate) add_actions_count: i64,
}

impl HasSelectionVector for CheckpointBatch {
    fn has_selected_rows(&self) -> bool {
        self.filtered_data.has_selected_rows()
    }
}

impl LogReplayProcessor for CheckpointLogReplayProcessor {
    type Output = CheckpointBatch;

    /// Processes a batch of actions read from the log during reverse chronological replay
    /// and returns a [`CheckpointBatch`], which contains the filtered actions to be
    /// included in the checkpoint file, along with statistics about the included actions.
    ///
    /// This method delegates the filtering logic to the [`CheckpointVisitor`], which implements
    /// the deduplication rules described in the module documentation. The method tracks
    /// statistics about processed actions (total count, add actions count) and maintains
    /// state for cross-batch deduplication.
    fn process_actions_batch(
        &mut self,
        batch: Box<dyn EngineData>,
        is_log_batch: bool,
    ) -> DeltaResult<Self::Output> {
        let selection_vector = vec![true; batch.len()];

        // Create the checkpoint visitor to process actions and update selection vector
        let mut visitor = CheckpointVisitor::new(
            &mut self.seen_file_keys,
            is_log_batch,
            selection_vector,
            self.minimum_file_retention_timestamp,
            self.seen_protocol,
            self.seen_metadata,
            &mut self.seen_txns,
        );
        visitor.visit_rows_of(batch.as_ref())?;

        // Update protocol and metadata seen flags
        self.seen_protocol = visitor.seen_protocol;
        self.seen_metadata = visitor.seen_metadata;

        let filtered_data = FilteredEngineData {
            data: batch,
            selection_vector: visitor.selection_vector,
        };

        Ok(CheckpointBatch {
            filtered_data,
            actions_count: visitor.actions_count,
            add_actions_count: visitor.add_actions_count,
        })
    }

    /// We never do data skipping for checkpoint log replay (entire table state is always reproduced)
    fn data_skipping_filter(&self) -> Option<&DataSkippingFilter> {
        None
    }
}

impl CheckpointLogReplayProcessor {
    pub(crate) fn new(minimum_file_retention_timestamp: i64) -> Self {
        Self {
            seen_file_keys: Default::default(),
            seen_protocol: false,
            seen_metadata: false,
            seen_txns: Default::default(),
            minimum_file_retention_timestamp,
        }
    }
}

/// A visitor that filters actions for inclusion in a V1 spec checkpoint file.
///
/// This visitor processes actions in newest-to-oldest order (as they appear in log
/// replay) and applies deduplication logic for both file and non-file actions to
/// produce the actions to include in a checkpoint.
///
/// # File Action Filtering Rules:
///   Kept Actions:
/// - The first (newest) add action for each unique (path, dvId) pair
/// - The first (newest) remove action for each unique (path, dvId) pair, but only if
///   its deletionTimestamp > minimumFileRetentionTimestamp
///   Omitted Actions:
/// - Any file action (add/remove) with the same (path, dvId) as a previously processed action
/// - All remove actions with deletionTimestamp ≤ minimumFileRetentionTimestamp
/// - All remove actions with missing deletionTimestamp (defaults to 0)
///
/// The resulting filtered file actions represents files present in the table (add actions) and
/// unexpired tombstones required for vacuum operations (remove actions).
///
/// # Non-File Action Filtering:
/// - Keeps only the first protocol action (newest version)
/// - Keeps only the first metadata action (most recent table metadata)
/// - Keeps only the first txn action for each unique app ID
///
/// # Excluded Actions
/// - CommitInfo, CDC, and CheckpointMetadata actions should not appear in the action
///   batches processed by this visitor, as they are excluded by the schema used to
///   read the log files upstream. If present, they will be ignored by the visitor.
/// - Sidecar actions should also be excluded—when encountered in the log, the
///   corresponding sidecar files are read to extract the referenced file actions,
///   which are then included directly in the action stream instead of the sidecar actions themselves.
/// - The CheckpointMetadata action is included down the wire when writing a V2 spec checkpoint.
///
/// # Memory Usage
/// This struct has O(N + M) memory usage where:
/// - N = number of txn actions with unique appIds
/// - M = number of file actions with unique (path, dvId) pairs
///
/// The resulting filtered set of actions are the actions which should be written to a
/// checkpoint for a corresponding version.
pub(crate) struct CheckpointVisitor<'seen> {
    // Deduplicates file actions (applies logic to filter Adds with corresponding Removes,
    // and keep unexpired Removes). This deduplicator builds a set of seen file actions.
    // This set has O(M) memory usage where M = number of file actions with unique (path, dvId) pairs
    deduplicator: FileActionDeduplicator<'seen>,
    // Tracks which rows to include in the final output
    selection_vector: Vec<bool>,
    // TODO: _last_checkpoint schema should be updated to use u64 instead of i64
    // for fields that are not expected to be negative. (Issue #786)
    // i64 to match the `_last_checkpoint` file schema
    actions_count: i64,
    // i64 to match the `_last_checkpoint` file schema
    add_actions_count: i64,
    // i64 for comparison with remove.deletionTimestamp
    minimum_file_retention_timestamp: i64,
    // Flag to track if we've seen a protocol action so we can keep only the first protocol action
    seen_protocol: bool,
    // Flag to track if we've seen a metadata action so we can keep only the first metadata action
    seen_metadata: bool,
    // Set of transaction IDs to deduplicate by appId
    // This set has O(N) memory usage where N = number of txn actions with unique appIds
    seen_txns: &'seen mut HashSet<String>,
}

#[allow(unused)]
impl CheckpointVisitor<'_> {
    // These index positions correspond to the order of columns defined in
    // `selected_column_names_and_types()`
    const ADD_PATH_INDEX: usize = 0; // Position of "add.path" in getters
    const ADD_DV_START_INDEX: usize = 1; // Start position of add deletion vector columns
    const REMOVE_PATH_INDEX: usize = 4; // Position of "remove.path" in getters
    const REMOVE_DELETION_TIMESTAMP_INDEX: usize = 5; // Position of "remove.deletionTimestamp" in getters
    const REMOVE_DV_START_INDEX: usize = 6; // Start position of remove deletion vector columns

    // These are the column names used to access the data in the getters
    const REMOVE_DELETION_TIMESTAMP: &'static str = "remove.deletionTimestamp";
    const PROTOCOL_MIN_READER_VERSION: &'static str = "protocol.minReaderVersion";
    const METADATA_ID: &'static str = "metaData.id";

    pub(crate) fn new<'seen>(
        seen_file_keys: &'seen mut HashSet<FileActionKey>,
        is_log_batch: bool,
        selection_vector: Vec<bool>,
        minimum_file_retention_timestamp: i64,
        seen_protocol: bool,
        seen_metadata: bool,
        seen_txns: &'seen mut HashSet<String>,
    ) -> CheckpointVisitor<'seen> {
        CheckpointVisitor {
            deduplicator: FileActionDeduplicator::new(
                seen_file_keys,
                is_log_batch,
                Self::ADD_PATH_INDEX,
                Self::REMOVE_PATH_INDEX,
                Self::ADD_DV_START_INDEX,
                Self::REMOVE_DV_START_INDEX,
            ),
            selection_vector,
            actions_count: 0,
            add_actions_count: 0,
            minimum_file_retention_timestamp,
            seen_protocol,
            seen_metadata,
            seen_txns,
        }
    }

    /// Determines if a remove action tombstone has expired and should be excluded from the checkpoint.
    ///
    /// A remove action includes a deletion_timestamp indicating when the deletion occurred. Physical
    /// files are deleted lazily after a user-defined expiration time. Remove actions are kept to allow
    /// concurrent readers to read snapshots at older versions.
    ///
    /// Tombstone expiration rules:
    /// - If deletion_timestamp <= minimum_file_retention_timestamp: Expired (exclude)
    /// - If deletion_timestamp > minimum_file_retention_timestamp: Valid (include)
    /// - If deletion_timestamp is missing: Defaults to 0, treated as expired (exclude)
    fn is_expired_tombstone<'a>(&self, i: usize, getter: &'a dyn GetData<'a>) -> DeltaResult<bool> {
        // Ideally this should never be zero, but we are following the same behavior as Delta
        // Spark and the Java Kernel.
        // Note: When remove.deletion_timestamp is not present (defaulting to 0), the remove action
        // will be excluded from the checkpoint file as it will be treated as expired.
        let deletion_timestamp = getter.get_opt(i, "remove.deletionTimestamp")?;
        let deletion_timestamp = deletion_timestamp.unwrap_or(0i64);

        Ok(deletion_timestamp <= self.minimum_file_retention_timestamp)
    }

    /// Processes a potential file action to determine if it should be included in the checkpoint.
    ///
    /// Returns Ok(true) if the row contains a valid file action to be included in the checkpoint.
    /// Returns Ok(false) if the row doesn't contain a file action or should be skipped.
    /// Returns Err(...) if there was an error processing the action.
    ///
    /// Note: This function handles both add and remove actions, applying deduplication logic and
    /// tombstone expiration rules as needed.
    fn check_file_action<'a>(
        &mut self,
        i: usize,
        getters: &[&'a dyn GetData<'a>],
    ) -> DeltaResult<bool> {
        // Extract the file action and handle errors immediately
        let (file_key, is_add) = match self.deduplicator.extract_file_action(i, getters, false)? {
            Some(action) => action,
            None => return Ok(false), // If no file action is found, skip this row
        };

        // Check if we've already seen this file action
        if self.deduplicator.check_and_record_seen(file_key) {
            return Ok(false); // Skip file actions that we've processed before
        }

        // Check for valid, non-duplicate adds and non-expired removes
        if is_add {
            self.add_actions_count += 1;
        } else if self.is_expired_tombstone(i, getters[Self::REMOVE_DELETION_TIMESTAMP_INDEX])? {
            return Ok(false); // Skip expired remove tombstones
        }
        Ok(true) // Include this action
    }

    /// Processes a potential protocol action to determine if it should be included in the checkpoint.
    ///
    /// Returns Ok(true) if the row contains a valid protocol action.
    /// Returns Ok(false) if the row doesn't contain a protocol action or is a duplicate.
    /// Returns Err(...) if there was an error processing the action.
    fn check_protocol_action<'a>(
        &mut self,
        i: usize,
        getter: &'a dyn GetData<'a>,
    ) -> DeltaResult<bool> {
        // Skip protocol actions if we've already seen a newer one
        if self.seen_protocol {
            return Ok(false);
        }

        // minReaderVersion is a required field, so we check for its presence to determine if this is a protocol action.
        if getter
            .get_int(i, Self::PROTOCOL_MIN_READER_VERSION)?
            .is_none()
        {
            return Ok(false); // Not a protocol action
        }
        // Valid, non-duplicate protocol action to be included
        self.seen_protocol = true;
        Ok(true)
    }

    /// Processes a potential metadata action to determine if it should be included in the checkpoint.
    ///
    /// Returns Ok(true) if the row contains a valid metadata action.
    /// Returns Ok(false) if the row doesn't contain a metadata action or is a duplicate.
    /// Returns Err(...) if there was an error processing the action.
    fn check_metadata_action<'a>(
        &mut self,
        i: usize,
        getter: &'a dyn GetData<'a>,
    ) -> DeltaResult<bool> {
        // Skip metadata actions if we've already seen a newer one
        if self.seen_metadata {
            return Ok(false);
        }

        // id is a required field, so we check for its presence to determine if this is a metadata action.
        if getter.get_str(i, Self::METADATA_ID)?.is_none() {
            return Ok(false); // Not a metadata action
        }

        // Valid, non-duplicate metadata action to be included
        self.seen_metadata = true;
        Ok(true)
    }

    /// Processes a potential txn action to determine if it should be included in the checkpoint.
    ///
    /// Returns Ok(true) if the row contains a valid txn action.
    /// Returns Ok(false) if the row doesn't contain a txn action or is a duplicate.
    /// Returns Err(...) if there was an error processing the action.
    fn check_txn_action<'a>(&mut self, i: usize, getter: &'a dyn GetData<'a>) -> DeltaResult<bool> {
        // Check for txn field
        let Some(app_id) = getter.get_str(i, "txn.appId")? else {
            return Ok(false); // Not a txn action
        };

        // If the app ID already exists in the set, the insertion will return false,
        // indicating that this is a duplicate.
        if !self.seen_txns.insert(app_id.to_string()) {
            return Ok(false);
        }

        // Valid, non-duplicate txn action to be included
        Ok(true)
    }

    /// Determines if a row in the batch should be included in the checkpoint.
    ///
    /// This method checks each action type in sequence, short-circuiting as soon as a valid action is found.
    /// Actions are checked in order of expected frequency of occurrence to optimize performance:
    /// 1. File actions (most frequent)
    /// 2. Txn actions
    /// 3. Protocol & Metadata actions (least frequent)
    ///
    /// Returns Ok(true) if the row should be included in the checkpoint.
    /// Returns Ok(false) if the row should be skipped.
    /// Returns Err(...) if any validation or extraction failed.
    pub(crate) fn is_valid_action<'a>(
        &mut self,
        i: usize,
        getters: &[&'a dyn GetData<'a>],
    ) -> DeltaResult<bool> {
        // The `||` operator short-circuits the evaluation, so if any of the checks return true,
        // the rest will not be evaluated.
        let is_valid = self.check_file_action(i, getters)?
            || self.check_txn_action(i, getters[11])?
            || self.check_protocol_action(i, getters[10])?
            || self.check_metadata_action(i, getters[9])?;

        if is_valid {
            self.actions_count += 1;
        }

        Ok(is_valid)
    }
}

impl RowVisitor for CheckpointVisitor<'_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        // The data columns visited must be in the following order:
        // 1. ADD
        // 2. REMOVE
        // 3. METADATA
        // 4. PROTOCOL
        // 5. TXN
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            const STRING: DataType = DataType::STRING;
            const INTEGER: DataType = DataType::INTEGER;
            const LONG: DataType = DataType::LONG;
            let types_and_names = vec![
                // File action columns
                (STRING, column_name!("add.path")),
                (STRING, column_name!("add.deletionVector.storageType")),
                (STRING, column_name!("add.deletionVector.pathOrInlineDv")),
                (INTEGER, column_name!("add.deletionVector.offset")),
                (STRING, column_name!("remove.path")),
                (LONG, column_name!("remove.deletionTimestamp")),
                (STRING, column_name!("remove.deletionVector.storageType")),
                (STRING, column_name!("remove.deletionVector.pathOrInlineDv")),
                (INTEGER, column_name!("remove.deletionVector.offset")),
                // Non-file action columns
                (STRING, column_name!("metaData.id")),
                (INTEGER, column_name!("protocol.minReaderVersion")),
                (STRING, column_name!("txn.appId")),
            ];
            let (types, names) = types_and_names.into_iter().unzip();
            (names, types).into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        require!(
            getters.len() == 12,
            Error::InternalError(format!(
                "Wrong number of visitor getters: {}",
                getters.len()
            ))
        );

        for i in 0..row_count {
            self.selection_vector[i] = self.is_valid_action(i, getters)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::arrow::array::StringArray;
    use crate::utils::test_utils::{action_batch, parse_json_batch};

    use itertools::Itertools;

    /// Helper function to create test batches from JSON strings
    fn create_batch(json_strings: Vec<&str>) -> DeltaResult<(Box<dyn EngineData>, bool)> {
        Ok((parse_json_batch(StringArray::from(json_strings)), true))
    }

    /// Helper function which applies the [`CheckpointLogReplayProcessor`] to a set of
    /// input batches and returns the results.
    fn run_checkpoint_test(
        input_batches: Vec<(Box<dyn EngineData>, bool)>,
    ) -> DeltaResult<(Vec<FilteredEngineData>, i64, i64)> {
        let processed_batches: Vec<_> = CheckpointLogReplayProcessor::new(0)
            .process_actions_iter(input_batches.into_iter().map(Ok))
            .try_collect()?;
        let total_count: i64 = processed_batches.iter().map(|b| b.actions_count).sum();
        let add_count: i64 = processed_batches.iter().map(|b| b.add_actions_count).sum();
        let filtered_data = processed_batches
            .into_iter()
            .map(|b| b.filtered_data)
            .collect();

        Ok((filtered_data, total_count, add_count))
    }
    #[test]
    fn test_checkpoint_visitor() -> DeltaResult<()> {
        let data = action_batch();
        let mut seen_file_keys = HashSet::new();
        let mut seen_txns = HashSet::new();
        let mut visitor = CheckpointVisitor::new(
            &mut seen_file_keys,
            true,
            vec![true; 9],
            0, // minimum_file_retention_timestamp (no expired tombstones)
            false,
            false,
            &mut seen_txns,
        );

        visitor.visit_rows_of(data.as_ref())?;

        let expected = vec![
            true,  // Row 0 is an add action (included)
            true,  // Row 1 is a remove action (included)
            false, // Row 2 is a commit info action (excluded)
            true,  // Row 3 is a protocol action (included)
            true,  // Row 4 is a metadata action (included)
            false, // Row 5 is a cdc action (excluded)
            false, // Row 6 is a sidecar action (excluded)
            true,  // Row 7 is a txn action (included)
            false, // Row 8 is a checkpointMetadata action (excluded)
        ];

        assert_eq!(visitor.actions_count, 5);
        assert_eq!(visitor.add_actions_count, 1);
        assert!(visitor.seen_protocol);
        assert!(visitor.seen_metadata);
        assert_eq!(visitor.seen_txns.len(), 1);

        assert_eq!(visitor.selection_vector, expected);
        Ok(())
    }

    /// Tests the boundary conditions for tombstone expiration logic.
    /// Specifically checks:
    /// - Remove actions with deletionTimestamp == minimumFileRetentionTimestamp (should be excluded)
    /// - Remove actions with deletionTimestamp < minimumFileRetentionTimestamp (should be excluded)
    /// - Remove actions with deletionTimestamp > minimumFileRetentionTimestamp (should be included)
    /// - Remove actions with missing deletionTimestamp (defaults to 0, should be excluded)
    #[test]
    fn test_checkpoint_visitor_boundary_cases_for_tombstone_expiration() -> DeltaResult<()> {
        let json_strings: StringArray = vec![
            r#"{"remove":{"path":"exactly_at_threshold","deletionTimestamp":100,"dataChange":true,"partitionValues":{}}}"#,
            r#"{"remove":{"path":"one_below_threshold","deletionTimestamp":99,"dataChange":true,"partitionValues":{}}}"#,
            r#"{"remove":{"path":"one_above_threshold","deletionTimestamp":101,"dataChange":true,"partitionValues":{}}}"#,
            // Missing timestamp defaults to 0
            r#"{"remove":{"path":"missing_timestamp","dataChange":true,"partitionValues":{}}}"#,
        ]
        .into();
        let batch = parse_json_batch(json_strings);

        let mut seen_file_keys = HashSet::new();
        let mut seen_txns = HashSet::new();
        let mut visitor = CheckpointVisitor::new(
            &mut seen_file_keys,
            true,
            vec![true; 4],
            100, // minimum_file_retention_timestamp (threshold set to 100)
            false,
            false,
            &mut seen_txns,
        );

        visitor.visit_rows_of(batch.as_ref())?;

        // Only "one_above_threshold" should be kept
        let expected = vec![false, false, true, false];
        assert_eq!(visitor.selection_vector, expected);
        assert_eq!(visitor.actions_count, 1);
        assert_eq!(visitor.add_actions_count, 0);
        Ok(())
    }

    #[test]
    fn test_checkpoint_visitor_file_actions_in_checkpoint_batch() -> DeltaResult<()> {
        let json_strings: StringArray = vec![
            r#"{"add":{"path":"file1","partitionValues":{"c1":"6","c2":"a"},"size":452,"modificationTime":1670892998137,"dataChange":true}}"#,
        ]
        .into();
        let batch = parse_json_batch(json_strings);

        let mut seen_file_keys = HashSet::new();
        let mut seen_txns = HashSet::new();
        let mut visitor = CheckpointVisitor::new(
            &mut seen_file_keys,
            false, // is_log_batch = false (checkpoint batch)
            vec![true; 1],
            0,
            false,
            false,
            &mut seen_txns,
        );

        visitor.visit_rows_of(batch.as_ref())?;

        let expected = vec![true];
        assert_eq!(visitor.selection_vector, expected);
        assert_eq!(visitor.actions_count, 1);
        assert_eq!(visitor.add_actions_count, 1);
        // The action should NOT be added to the seen_file_keys set as it's a checkpoint batch
        // and actions in checkpoint batches do not conflict with each other.
        // This is a key difference from log batches, where actions can conflict.
        assert!(seen_file_keys.is_empty());
        Ok(())
    }

    #[test]
    fn test_checkpoint_visitor_file_actions_with_deletion_vectors() -> DeltaResult<()> {
        let json_strings: StringArray = vec![
            // Add action for file1 with deletion vector
            r#"{"add":{"path":"file1","partitionValues":{},"size":635,"modificationTime":100,"dataChange":true,"deletionVector":{"storageType":"ONE","pathOrInlineDv":"dv1","offset":1,"sizeInBytes":36,"cardinality":2}}}"#,
            // Remove action for file1 with a different deletion vector
            r#"{"remove":{"path":"file1","deletionTimestamp":100,"dataChange":true,"deletionVector":{"storageType":"TWO","pathOrInlineDv":"dv2","offset":1,"sizeInBytes":36,"cardinality":2}}}"#,
            // Remove action for file1 with another different deletion vector
            r#"{"remove":{"path":"file1","deletionTimestamp":100,"dataChange":true,"deletionVector":{"storageType":"THREE","pathOrInlineDv":"dv3","offset":1,"sizeInBytes":36,"cardinality":2}}}"#,
         ]
        .into();
        let batch = parse_json_batch(json_strings);

        let mut seen_file_keys = HashSet::new();
        let mut seen_txns = HashSet::new();
        let mut visitor = CheckpointVisitor::new(
            &mut seen_file_keys,
            true,
            vec![true; 3],
            0,
            false,
            false,
            &mut seen_txns,
        );

        visitor.visit_rows_of(batch.as_ref())?;

        let expected = vec![true, true, true];
        assert_eq!(visitor.selection_vector, expected);
        assert_eq!(visitor.actions_count, 3);
        assert_eq!(visitor.add_actions_count, 1);

        Ok(())
    }

    #[test]
    fn test_checkpoint_visitor_already_seen_non_file_actions() -> DeltaResult<()> {
        let json_strings: StringArray = vec![
            r#"{"txn":{"appId":"app1","version":1,"lastUpdated":123456789}}"#,
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}"#,
            r#"{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1677811175819}}"#,
        ].into();
        let batch = parse_json_batch(json_strings);

        // Pre-populate with txn app1
        let mut seen_file_keys = HashSet::new();
        let mut seen_txns = HashSet::new();
        seen_txns.insert("app1".to_string());

        let mut visitor = CheckpointVisitor::new(
            &mut seen_file_keys,
            true,
            vec![true; 3],
            0,
            true,           // The visior has already seen a protocol action
            true,           // The visitor has already seen a metadata action
            &mut seen_txns, // Pre-populated transaction
        );

        visitor.visit_rows_of(batch.as_ref())?;

        // All actions should be skipped as they have already been seen
        let expected = vec![false, false, false];
        assert_eq!(visitor.selection_vector, expected);
        assert_eq!(visitor.actions_count, 0);

        Ok(())
    }

    #[test]
    fn test_checkpoint_visitor_duplicate_non_file_actions() -> DeltaResult<()> {
        let json_strings: StringArray = vec![
            r#"{"txn":{"appId":"app1","version":1,"lastUpdated":123456789}}"#,
            r#"{"txn":{"appId":"app1","version":1,"lastUpdated":123456789}}"#, // Duplicate txn
            r#"{"txn":{"appId":"app2","version":1,"lastUpdated":123456789}}"#, // Different app ID
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7}}"#,
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7}}"#, // Duplicate protocol
            r#"{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1677811175819}}"#,
            // Duplicate metadata
            r#"{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1677811175819}}"#,
        ]
        .into();
        let batch = parse_json_batch(json_strings);

        let mut seen_file_keys = HashSet::new();
        let mut seen_txns = HashSet::new();
        let mut visitor = CheckpointVisitor::new(
            &mut seen_file_keys,
            true, // is_log_batch
            vec![true; 7],
            0, // minimum_file_retention_timestamp
            false,
            false,
            &mut seen_txns,
        );

        visitor.visit_rows_of(batch.as_ref())?;

        // First occurrence of each type should be included
        let expected = vec![true, false, true, true, false, true, false];
        assert_eq!(visitor.selection_vector, expected);
        assert_eq!(visitor.seen_txns.len(), 2); // Two different app IDs
        assert_eq!(visitor.actions_count, 4); // 2 txns + 1 protocol + 1 metadata

        Ok(())
    }

    /// This test ensures that the processor correctly deduplicates and filters
    /// non-file actions (metadata, protocol, txn) across multiple batches.
    #[test]
    fn test_checkpoint_actions_iter_non_file_actions() -> DeltaResult<()> {
        // Batch 1: protocol, metadata, and txn actions
        let batch1 = vec![
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            r#"{"metaData":{"id":"test1","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"c1\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c2\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c3\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["c1","c2"],"configuration":{},"createdTime":1670892997849}}"#,
            r#"{"txn":{"appId":"app1","version":1,"lastUpdated":123456789}}"#,
        ];

        // Batch 2: duplicate actions, and a new txn action
        let batch2 = vec![
            // Duplicates that should be skipped
            r#"{"protocol":{"minReaderVersion":2,"minWriterVersion":3}}"#,
            r#"{"metaData":{"id":"test2","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"c1\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c2\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c3\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["c1","c2"],"configuration":{},"createdTime":1670892997849}}"#,
            r#"{"txn":{"appId":"app1","version":1,"lastUpdated":123456789}}"#,
            // Unique transaction (appId) should be included
            r#"{"txn":{"appId":"app2","version":1,"lastUpdated":123456789}}"#,
        ];

        // Batch 3: a duplicate action (entire batch should be skipped)
        let batch3 = vec![r#"{"protocol":{"minReaderVersion":2,"minWriterVersion":3}}"#];

        let input_batches = vec![
            create_batch(batch1)?,
            create_batch(batch2)?,
            create_batch(batch3)?,
        ];
        let (results, actions_count, add_actions) = run_checkpoint_test(input_batches)?;

        // Verify results
        assert_eq!(results.len(), 2, "Expected two batches in results");
        assert_eq!(results[0].selection_vector, vec![true, true, true],);
        assert_eq!(results[1].selection_vector, vec![false, false, false, true],);
        assert_eq!(actions_count, 4);
        assert_eq!(add_actions, 0);

        Ok(())
    }

    /// This test ensures that the processor correctly deduplicates and filters
    /// file actions (add, remove) across multiple batches.
    #[test]
    fn test_checkpoint_actions_iter_file_actions() -> DeltaResult<()> {
        // Batch 1: add action (file1) - new, should be included
        let batch1 = vec![
            r#"{"add":{"path":"file1","partitionValues":{"c1":"6","c2":"a"},"size":452,"modificationTime":1670892998137,"dataChange":true}}"#,
        ];

        // Batch 2: remove actions - mixed inclusion
        let batch2 = vec![
            // Already seen file, should be excluded
            r#"{"remove":{"path":"file1","deletionTimestamp":100,"dataChange":true,"partitionValues":{}}}"#,
            // New file, should be included
            r#"{"remove":{"path":"file2","deletionTimestamp":100,"dataChange":true,"partitionValues":{}}}"#,
        ];

        // Batch 3: add action (file2) - already seen, should be excluded
        let batch3 = vec![
            r#"{"add":{"path":"file2","partitionValues":{"c1":"6","c2":"a"},"size":452,"modificationTime":1670892998137,"dataChange":true}}"#,
        ];

        let input_batches = vec![
            create_batch(batch1)?,
            create_batch(batch2)?,
            create_batch(batch3)?,
        ];
        let (results, actions_count, add_actions) = run_checkpoint_test(input_batches)?;

        // Verify results
        assert_eq!(results.len(), 2); // The third batch should be filtered out since there are no selected actions
        assert_eq!(results[0].selection_vector, vec![true]);
        assert_eq!(results[1].selection_vector, vec![false, true]);
        assert_eq!(actions_count, 2);
        assert_eq!(add_actions, 1);

        Ok(())
    }

    /// This test ensures that the processor correctly deduplicates and filters
    /// file actions (add, remove) with deletion vectors across multiple batches.
    #[test]
    fn test_checkpoint_actions_iter_file_actions_with_deletion_vectors() -> DeltaResult<()> {
        // Batch 1: add actions with deletion vectors
        let batch1 = vec![
            // (file1, DV_ONE) New, should be included
            r#"{"add":{"path":"file1","partitionValues":{},"size":635,"modificationTime":100,"dataChange":true,"deletionVector":{"storageType":"ONE","pathOrInlineDv":"dv1","offset":1,"sizeInBytes":36,"cardinality":2}}}"#,
            // (file1, DV_TWO) New, should be included
            r#"{"add":{"path":"file1","partitionValues":{},"size":635,"modificationTime":100,"dataChange":true,"deletionVector":{"storageType":"TWO","pathOrInlineDv":"dv2","offset":1,"sizeInBytes":36,"cardinality":2}}}"#,
        ];

        // Batch 2: mixed actions with duplicate and new entries
        let batch2 = vec![
            // (file1, DV_ONE): Already seen, should be excluded
            r#"{"remove":{"path":"file1","deletionTimestamp":100,"dataChange":true,"deletionVector":{"storageType":"ONE","pathOrInlineDv":"dv1","offset":1,"sizeInBytes":36,"cardinality":2}}}"#,
            // (file1, DV_TWO): Already seen, should be excluded
            r#"{"add":{"path":"file1","partitionValues":{},"size":635,"modificationTime":100,"dataChange":true,"deletionVector":{"storageType":"TWO","pathOrInlineDv":"dv2","offset":1,"sizeInBytes":36,"cardinality":2}}}"#,
            // New file, should be included
            r#"{"remove":{"path":"file2","deletionTimestamp":100,"dataChange":true,"partitionValues":{}}}"#,
        ];

        let input_batches = vec![create_batch(batch1)?, create_batch(batch2)?];
        let (results, actions_count, add_actions) = run_checkpoint_test(input_batches)?;

        // Verify results
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].selection_vector, vec![true, true]);
        assert_eq!(results[1].selection_vector, vec![false, false, true]);
        assert_eq!(actions_count, 3);
        assert_eq!(add_actions, 2);

        Ok(())
    }
}
