//! This module implements log replay functionality specifically for checkpoint writes in delta tables.
//!
//! The primary goal is to process Delta log actions in reverse chronological order (from most recent to
//! least recent) to produce the minimal set of actions required to reconstruct the table state in a V1 checkpoint.
//!
//! ## Key Responsibilities
//! - Filtering: Only the most recent protocol and metadata actions are retained, and for each transaction
//!   (identified by its app ID), only the latest action is kept.
//! - Deduplication: File actions are deduplicated based on file path and deletion vector unique ID so that
//!   duplicate or obsolete actions (including remove actions) are ignored.
//! - Retention Filtering: Tombstones older than the configured `minimum_file_retention_timestamp` are excluded.
//!
//! The module defines the [`V1CheckpointLogReplayProccessor`] which implements the LogReplayProcessor trait,
//! as well as a [`V1CheckpointVisitor`] to traverse and process batches of log actions.
//!
//! The processing result is encapsulated in [`CheckpointData`], which includes the transformed log data and
//! a selection vector indicating which rows should be written to the checkpoint.
//!
//! For log replay functionality used during table scans (i.e. for reading checkpoints and commit logs), refer to
//! the `scan/log_replay.rs` module.
use std::collections::HashSet;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, LazyLock};

use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::log_replay::{
    FileActionDeduplicator, FileActionKey, HasSelectionVector, LogReplayProcessor,
};
use crate::schema::{column_name, ColumnName, ColumnNamesAndTypes, DataType};
use crate::utils::require;
use crate::{DeltaResult, EngineData, Error};

/// `CheckpointData` contains a batch of filtered actions for checkpoint creation.
/// This structure holds a single batch of engine data along with a selection vector
/// that marks which rows should be included in the V1 checkpoint file.
/// TODO!(seb): change to type CheckpointData = FilteredEngineData, when introduced
pub struct CheckpointData {
    /// The original engine data containing the actions
    #[allow(dead_code)] // TODO: Remove once checkpoint_v1 API is implemented
    data: Box<dyn EngineData>,
    /// Boolean vector indicating which rows should be included in the checkpoint
    selection_vector: Vec<bool>,
}

impl HasSelectionVector for CheckpointData {
    /// Returns true if any row in the selection vector is marked as selected
    fn has_selected_rows(&self) -> bool {
        self.selection_vector.contains(&true)
    }
}

/// The [`V1CheckpointLogReplayProccessor`] is an implementation of the [`LogReplayProcessor`]
/// trait that filters log segment actions for inclusion in a V1 spec checkpoint file.
///
/// It processes each action batch via the `process_actions_batch` method, using the
/// [`V1CheckpointVisitor`] to convert each batch into a [`CheckpointData`] instance that
/// contains only the actions required for the checkpoint.
pub(crate) struct V1CheckpointLogReplayProccessor {
    /// Tracks file actions that have been seen during log replay to avoid duplicates.
    /// Contains (data file path, dv_unique_id) pairs as `FileActionKey` instances.
    seen_file_keys: HashSet<FileActionKey>,

    /// Counter for the total number of actions processed during log replay.
    total_actions: Arc<AtomicI64>,

    /// Counter for the total number of add actions processed during log replay.
    total_add_actions: Arc<AtomicI64>,

    /// Indicates whether a protocol action has been seen in the log.
    seen_protocol: bool,

    /// Indicates whether a metadata action has been seen in the log.
    seen_metadata: bool,

    /// Set of transaction app IDs that have been processed to avoid duplicates.
    seen_txns: HashSet<String>,

    /// Minimum timestamp for file retention, used for filtering expired tombstones.
    minimum_file_retention_timestamp: i64,
}

impl LogReplayProcessor for V1CheckpointLogReplayProccessor {
    // Define the processing result type as CheckpointData
    type Output = CheckpointData;

    /// This function processes batches of actions in reverse chronological order
    /// (from most recent to least recent) and performs the necessary filtering
    /// to ensure the checkpoint contains only the actions needed to reconstruct
    /// the complete state of the table.
    ///
    /// # Filtering Rules
    ///
    /// The following rules apply when filtering actions:
    ///
    /// 1. Only the most recent protocol and metadata actions are included
    /// 2. For each app ID, only the most recent transaction action is included
    /// 3. Add and remove actions are deduplicated based on path and unique ID
    /// 4. Tombstones older than `minimum_file_retention_timestamp` are excluded
    /// 5. Sidecar, commitInfo, and CDC actions are excluded
    fn process_actions_batch(
        &mut self,
        batch: Box<dyn EngineData>,
        is_log_batch: bool,
    ) -> DeltaResult<Self::Output> {
        // Initialize selection vector with all rows un-selected
        let selection_vector = vec![false; batch.len()];
        assert_eq!(
            selection_vector.len(),
            batch.len(),
            "Initial selection vector length does not match actions length"
        );

        // Create the checkpoint visitor to process actions and update selection vector
        let mut visitor = V1CheckpointVisitor::new(
            &mut self.seen_file_keys,
            is_log_batch,
            selection_vector,
            self.minimum_file_retention_timestamp,
            self.seen_protocol,
            self.seen_metadata,
            &mut self.seen_txns,
        );

        // Process actions and let visitor update selection vector
        visitor.visit_rows_of(batch.as_ref())?;

        // Update shared counters with file action counts from this batch
        self.total_actions.fetch_add(
            visitor.total_file_actions + visitor.total_non_file_actions,
            Ordering::SeqCst,
        );
        self.total_add_actions
            .fetch_add(visitor.total_add_actions, Ordering::SeqCst);

        // Update protocol and metadata seen flags
        self.seen_protocol = visitor.seen_protocol;
        self.seen_metadata = visitor.seen_metadata;

        Ok(CheckpointData {
            data: batch,
            selection_vector: visitor.selection_vector,
        })
    }
}

impl V1CheckpointLogReplayProccessor {
    pub(crate) fn new(
        total_actions_counter: Arc<AtomicI64>,
        total_add_actions_counter: Arc<AtomicI64>,
        minimum_file_retention_timestamp: i64,
    ) -> Self {
        Self {
            seen_file_keys: Default::default(),
            total_actions: total_actions_counter,
            total_add_actions: total_add_actions_counter,
            seen_protocol: false,
            seen_metadata: false,
            seen_txns: Default::default(),
            minimum_file_retention_timestamp,
        }
    }
}

/// Given an iterator of (engine_data, bool) tuples, returns an iterator of
/// `(engine_data, selection_vec)`. Each row that is selected in the returned `engine_data` _must_
/// be written to the V1 checkpoint file in order to capture the table version's complete state.
/// Non-selected rows _must_ be ignored. The boolean flag indicates whether the record batch
/// is a log or checkpoint batch.
///
/// Note: The iterator of (engine_data, bool) tuples 'action_iter' parameter must be sorted by the
/// order of the actions in the log from most recent to least recent.
#[allow(unused)] // TODO: Remove once checkpoint_v1 API is implemented
pub(crate) fn checkpoint_actions_iter(
    action_iter: impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send + 'static,
    total_actions_counter: Arc<AtomicI64>,
    total_add_actions_counter: Arc<AtomicI64>,
    minimum_file_retention_timestamp: i64,
) -> impl Iterator<Item = DeltaResult<CheckpointData>> + Send + 'static {
    let log_scanner = V1CheckpointLogReplayProccessor::new(
        total_actions_counter,
        total_add_actions_counter,
        minimum_file_retention_timestamp,
    );
    V1CheckpointLogReplayProccessor::apply_to_iterator(log_scanner, action_iter)
}

/// A visitor that filters actions for inclusion in a V1 spec checkpoint file.
///
/// This visitor processes actions in newest-to-oldest order (as they appear in log
/// replay) and applies deduplication logic for both file and non-file actions to
/// produce the minimal state representation for the table.
///
/// # File Action Filtering
/// - Keeps only the first occurrence of each unique (path, dvId) pair
/// - Excludes expired tombstone remove actions (where deletionTimestamp ≤ minimumFileRetentionTimestamp)
/// - Add actions represent files present in the table
/// - Unexpired remove actions represent tombstones still needed for consistency
///
/// # Non-File Action Filtering
/// - Keeps only the first protocol action (newest version)
/// - Keeps only the first metadata action (most recent table metadata)
/// - Keeps only the first transaction action for each unique app ID
///
/// # Excluded Actions
/// CommitInfo, CDC, Sidecar, and CheckpointMetadata actions are NOT part of the V1 checkpoint schema
/// and are filtered out.
///
/// The resulting filtered set of actions represents the minimal set needed to reconstruct
/// the latest valid state of the table at the checkpointed version.
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
pub(crate) struct V1CheckpointVisitor<'seen> {
    // File actions state
    deduplicator: FileActionDeduplicator<'seen>, // Used to deduplicate file actions
    selection_vector: Vec<bool>,                 // Used to mark rows for selection
    total_file_actions: i64,                     // i64 to match the `_last_checkpoint` file schema
    total_add_actions: i64,                      // i64 to match the `_last_checkpoint` file schema
    minimum_file_retention_timestamp: i64,       // i64 for comparison with remove.deletionTimestamp

    // Non-file actions state
    seen_protocol: bool, // Used to keep only the first protocol action
    seen_metadata: bool, // Used to keep only the first metadata action
    seen_txns: &'seen mut HashSet<String>, // Used to keep only the first txn action for each app ID
    total_non_file_actions: i64, // i64 to match the `_last_checkpoint` file schema
}

#[allow(unused)]
impl V1CheckpointVisitor<'_> {
    // These index positions correspond to the order of columns defined in
    // `selected_column_names_and_types()`, and are used to extract file key information
    // for deduplication purposes
    const ADD_PATH_INDEX: usize = 0; // Position of "add.path" in getters
    const ADD_DV_START_INDEX: usize = 1; // Start position of add deletion vector columns
    const REMOVE_PATH_INDEX: usize = 4; // Position of "remove.path" in getters
    const REMOVE_DV_START_INDEX: usize = 6; // Start position of remove deletion vector columns

    pub(crate) fn new<'seen>(
        seen_file_keys: &'seen mut HashSet<FileActionKey>,
        is_log_batch: bool,
        selection_vector: Vec<bool>,
        minimum_file_retention_timestamp: i64,
        seen_protocol: bool,
        seen_metadata: bool,
        seen_txns: &'seen mut HashSet<String>,
    ) -> V1CheckpointVisitor<'seen> {
        V1CheckpointVisitor {
            deduplicator: FileActionDeduplicator::new(
                seen_file_keys,
                is_log_batch,
                Self::ADD_PATH_INDEX,
                Self::REMOVE_PATH_INDEX,
                Self::ADD_DV_START_INDEX,
                Self::REMOVE_DV_START_INDEX,
            ),
            selection_vector,
            total_file_actions: 0,
            total_add_actions: 0,
            minimum_file_retention_timestamp,

            seen_protocol,
            seen_metadata,
            seen_txns,
            total_non_file_actions: 0,
        }
    }

    /// Determines if a remove action tombstone has expired and should be excluded from the checkpoint.
    ///
    /// A remove action includes a timestamp indicating when the deletion occurred. Physical files  
    /// are deleted lazily after a user-defined expiration time, allowing concurrent readers to  
    /// access stale snapshots. A remove action remains as a tombstone in a checkpoint file until
    /// it expires, which happens when the deletion timestamp is less than or equal to the
    /// minimum file retention timestamp.
    ///
    /// Note: When remove.deletion_timestamp is not present (defaulting to 0), the remove action
    /// will be excluded from the checkpoint file as it will be treated as expired.
    fn is_expired_tombstone<'a>(&self, i: usize, getter: &'a dyn GetData<'a>) -> DeltaResult<bool> {
        // Ideally this should never be zero, but we are following the same behavior as Delta
        // Spark and the Java Kernel.
        // Note: When remove.deletion_timestamp is not present (defaulting to 0), the remove action
        // will be excluded from the checkpoint file as it will be treated as expired.
        let mut deletion_timestamp: i64 = 0;
        if let Some(ts) = getter.get_opt(i, "remove.deletionTimestamp")? {
            deletion_timestamp = ts;
        }

        Ok(deletion_timestamp <= self.minimum_file_retention_timestamp)
    }

    /// Returns true if the row contains a valid file action to be included in the checkpoint.
    /// This function handles both add and remove actions, applying deduplication logic and
    /// tombstone expiration rules as needed.
    fn is_valid_file_action<'a>(
        &mut self,
        i: usize,
        getters: &[&'a dyn GetData<'a>],
    ) -> DeltaResult<bool> {
        // Never skip remove actions, as they may be unexpired tombstones.
        let Some((file_key, is_add)) = self.deduplicator.extract_file_action(i, getters, false)?
        else {
            return Ok(false);
        };

        // Check if we've already seen this file action
        if self.deduplicator.check_and_record_seen(file_key) {
            return Ok(false);
        }

        // Ignore expired tombstones. The getter at the fifth index is the remove action's deletionTimestamp.
        if !is_add && self.is_expired_tombstone(i, getters[5])? {
            return Ok(false);
        }

        if is_add {
            self.total_add_actions += 1;
        }

        self.total_file_actions += 1;
        Ok(true)
    }

    /// Returns true if the row contains a protocol action, and we haven't seen one yet.
    fn is_valid_protocol_action<'a>(
        &mut self,
        i: usize,
        getter: &'a dyn GetData<'a>,
    ) -> DeltaResult<bool> {
        if getter.get_int(i, "protocol.minReaderVersion")?.is_some() && !self.seen_protocol {
            self.seen_protocol = true;
            self.total_non_file_actions += 1;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Returns true if the row contains a metadata action, and we haven't seen one yet.
    fn is_valid_metadata_action<'a>(
        &mut self,
        i: usize,
        getter: &'a dyn GetData<'a>,
    ) -> DeltaResult<bool> {
        if getter.get_str(i, "metaData.id")?.is_some() && !self.seen_metadata {
            self.seen_metadata = true;
            self.total_non_file_actions += 1;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Returns true if the row contains a txn action with an appId that we haven't seen yet.
    fn is_valid_txn_action<'a>(
        &mut self,
        i: usize,
        getter: &'a dyn GetData<'a>,
    ) -> DeltaResult<bool> {
        let app_id = match getter.get_str(i, "txn.appId")? {
            Some(id) => id,
            None => return Ok(false),
        };

        // Attempting to insert the app_id into the set. If it's already present, the insert will
        // return false, indicating that we've already seen this app_id.
        if self.seen_txns.insert(app_id.to_string()) {
            self.total_non_file_actions += 1;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl RowVisitor for V1CheckpointVisitor<'_> {
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
            let types_and_names = vec![
                // File action columns
                (STRING, column_name!("add.path")),
                (STRING, column_name!("add.deletionVector.storageType")),
                (STRING, column_name!("add.deletionVector.pathOrInlineDv")),
                (INTEGER, column_name!("add.deletionVector.offset")),
                (STRING, column_name!("remove.path")),
                (DataType::LONG, column_name!("remove.deletionTimestamp")),
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
            // Check for non-file actions (metadata, protocol, txn)
            let is_non_file_action = self.is_valid_metadata_action(i, getters[9])?
                || self.is_valid_protocol_action(i, getters[10])?
                || self.is_valid_txn_action(i, getters[11])?;

            // Check for file actions (add, remove)
            let is_file_action = self.is_valid_file_action(i, getters)?;

            // Mark the row for selection if it's either a valid non-file or file action
            if is_non_file_action || is_file_action {
                self.selection_vector[i] = true;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use crate::arrow::array::{RecordBatch, StringArray};
    use crate::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

    use super::*;
    use crate::{
        actions::get_log_schema, engine::arrow_data::ArrowEngineData, engine::sync::SyncEngine,
        Engine, EngineData,
    };

    // Helper function to convert a StringArray to EngineData
    fn string_array_to_engine_data(string_array: StringArray) -> Box<dyn EngineData> {
        let string_field = Arc::new(Field::new("a", DataType::Utf8, true));
        let schema = Arc::new(ArrowSchema::new(vec![string_field]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(string_array)])
            .expect("Can't convert to record batch");
        Box::new(ArrowEngineData::new(batch))
    }

    // Creates a batch of actions for testing
    fn action_batch() -> Box<dyn EngineData> {
        let json_strings: StringArray = vec![
            r#"{"add":{"path":"part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet","partitionValues":{},"size":635,"modificationTime":1677811178336,"dataChange":true,"stats":"{\"numRecords\":10,\"minValues\":{\"value\":0},\"maxValues\":{\"value\":9},\"nullCount\":{\"value\":0},\"tightBounds\":true}","tags":{"INSERTION_TIME":"1677811178336000","MIN_INSERTION_TIME":"1677811178336000","MAX_INSERTION_TIME":"1677811178336000","OPTIMIZE_TARGET_SIZE":"268435456"}}}"#,
            r#"{"remove":{"path":"part-00003-f525f459-34f9-46f5-82d6-d42121d883fd.c000.snappy.parquet","deletionTimestamp":1670892998135,"dataChange":true,"partitionValues":{"c1":"4","c2":"c"},"size":452}}"#, 
            r#"{"commitInfo":{"timestamp":1677811178585,"operation":"WRITE","operationParameters":{"mode":"ErrorIfExists","partitionBy":"[]"},"isolationLevel":"WriteSerializable","isBlindAppend":true,"operationMetrics":{"numFiles":"1","numOutputRows":"10","numOutputBytes":"635"},"engineInfo":"Databricks-Runtime/<unknown>","txnId":"a6a94671-55ef-450e-9546-b8465b9147de"}}"#,
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}"#,
            r#"{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableDeletionVectors":"true","delta.columnMapping.mode":"none", "delta.enableChangeDataFeed":"true"},"createdTime":1677811175819}}"#,
            r#"{"cdc":{"path":"_change_data/age=21/cdc-00000-93f7fceb-281a-446a-b221-07b88132d203.c000.snappy.parquet","partitionValues":{"age":"21"},"size":1033,"dataChange":false}}"#,
            r#"{"sidecar":{"path":"016ae953-37a9-438e-8683-9a9a4a79a395.parquet","sizeInBytes":9268,"modificationTime":1714496113961,"tags":{"tag_foo":"tag_bar"}}}"#,
            r#"{"txn":{"appId":"myApp","version": 3}}"#,
        ]
        .into();
        parse_json_batch(json_strings)
    }

    // Parses JSON strings into EngineData
    fn parse_json_batch(json_strings: StringArray) -> Box<dyn EngineData> {
        let engine = SyncEngine::new();
        let json_handler = engine.get_json_handler();
        let output_schema = get_log_schema().clone();
        json_handler
            .parse_json(string_array_to_engine_data(json_strings), output_schema)
            .unwrap()
    }

    #[test]
    fn test_v1_checkpoint_visitor() -> DeltaResult<()> {
        let data = action_batch();
        let mut seen_file_keys = HashSet::new();
        let mut seen_txns = HashSet::new();
        let mut visitor = V1CheckpointVisitor::new(
            &mut seen_file_keys,
            true,
            vec![false; 8],
            0, // minimum_file_retention_timestamp (no expired tombstones)
            false,
            false,
            &mut seen_txns,
        );

        visitor.visit_rows_of(data.as_ref())?;

        // Combined results from both file and non-file actions
        // Row 0 is an add action (included)
        // Row 1 is a remove action (included)
        // Row 2 is a commit info action (excluded)
        // Row 3 is a protocol action (included)
        // Row 4 is a metadata action (included)
        // Row 5 is a cdc action (excluded)
        // Row 6 is a sidecar action (excluded)
        // Row 7 is a txn action (included)
        let expected = vec![true, true, false, true, true, false, false, true];

        // Verify file action results
        assert_eq!(visitor.total_file_actions, 2);
        assert_eq!(visitor.total_add_actions, 1);

        // Verify non-file action results
        assert!(visitor.seen_protocol);
        assert!(visitor.seen_metadata);
        assert_eq!(visitor.seen_txns.len(), 1);
        assert_eq!(visitor.total_non_file_actions, 3);

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
    fn test_v1_checkpoint_visitor_boundary_cases_for_tombstone_expiration() -> DeltaResult<()> {
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
        let mut visitor = V1CheckpointVisitor::new(
            &mut seen_file_keys,
            true,
            vec![false; 4],
            100, // minimum_file_retention_timestamp (threshold set to 100)
            false,
            false,
            &mut seen_txns,
        );

        visitor.visit_rows_of(batch.as_ref())?;

        // Only "one_above_threshold" should be kept
        let expected = vec![false, false, true, false];
        assert_eq!(visitor.selection_vector, expected);
        assert_eq!(visitor.total_file_actions, 1);
        assert_eq!(visitor.total_add_actions, 0);
        assert_eq!(visitor.total_non_file_actions, 0);
        Ok(())
    }

    #[test]
    fn test_v1_checkpoint_visitor_conflicting_file_actions_in_log_batch() -> DeltaResult<()> {
        let json_strings: StringArray = vec![
            r#"{"add":{"path":"file1","partitionValues":{"c1":"6","c2":"a"},"size":452,"modificationTime":1670892998137,"dataChange":true}}"#,
             // Duplicate path
            r#"{"remove":{"path":"file1","deletionTimestamp":100,"dataChange":true,"partitionValues":{}}}"#,
        ]
        .into();
        let batch = parse_json_batch(json_strings);

        let mut seen_file_keys = HashSet::new();
        let mut seen_txns = HashSet::new();
        let mut visitor = V1CheckpointVisitor::new(
            &mut seen_file_keys,
            true,
            vec![false; 2],
            0,
            false,
            false,
            &mut seen_txns,
        );

        visitor.visit_rows_of(batch.as_ref())?;

        // First file action should be included. The second one should be excluded due to the conflict.
        let expected = vec![true, false];
        assert_eq!(visitor.selection_vector, expected);
        assert_eq!(visitor.total_file_actions, 1);
        assert_eq!(visitor.total_add_actions, 1);
        assert_eq!(visitor.total_non_file_actions, 0);
        Ok(())
    }

    #[test]
    fn test_v1_checkpoint_visitor_file_actions_in_checkpoint_batch() -> DeltaResult<()> {
        let json_strings: StringArray = vec![
            r#"{"add":{"path":"file1","partitionValues":{"c1":"6","c2":"a"},"size":452,"modificationTime":1670892998137,"dataChange":true}}"#,
        ]
        .into();
        let batch = parse_json_batch(json_strings);

        let mut seen_file_keys = HashSet::new();
        let mut seen_txns = HashSet::new();
        let mut visitor = V1CheckpointVisitor::new(
            &mut seen_file_keys,
            false, // is_log_batch = false (checkpoint batch)
            vec![false; 1],
            0,
            false,
            false,
            &mut seen_txns,
        );

        visitor.visit_rows_of(batch.as_ref())?;

        let expected = vec![true];
        assert_eq!(visitor.selection_vector, expected);
        assert_eq!(visitor.total_file_actions, 1);
        assert_eq!(visitor.total_add_actions, 1);
        assert_eq!(visitor.total_non_file_actions, 0);
        // The action should NOT be added to the seen_file_keys set as it's a checkpoint batch
        // and actions in checkpoint batches do not conflict with
        assert!(seen_file_keys.is_empty());
        Ok(())
    }

    #[test]
    fn test_v1_checkpoint_visitor_conflicts_with_deletion_vectors() -> DeltaResult<()> {
        let json_strings: StringArray = vec![
            r#"{"add":{"path":"file1","partitionValues":{},"size":635,"modificationTime":100,"dataChange":true,"deletionVector":{"storageType":"one","pathOrInlineDv":"vBn[lx{q8@P<9BNH/isA","offset":1,"sizeInBytes":36,"cardinality":2}}}"#,
            // Same path but different DV
            r#"{"add":{"path":"file1","partitionValues":{},"size":635,"modificationTime":100,"dataChange":true,"deletionVector":{"storageType":"two","pathOrInlineDv":"vBn[lx{q8@P<9BNH/isA","offset":1,"sizeInBytes":36,"cardinality":2}}}"#, 
            // Duplicate of first entry
            r#"{"add":{"path":"file1","partitionValues":{},"size":635,"modificationTime":100,"dataChange":true,"deletionVector":{"storageType":"one","pathOrInlineDv":"vBn[lx{q8@P<9BNH/isA","offset":1,"sizeInBytes":36,"cardinality":2}}}"#, 
            // Conflicting remove action with DV
            r#"{"remove":{"path":"file1","deletionTimestamp":100,"dataChange":true,"deletionVector":{"storageType":"one","pathOrInlineDv":"vBn[lx{q8@P<9BNH/isA","offset":1,"sizeInBytes":36,"cardinality":2}}}"#,
        ]
        .into();
        let batch = parse_json_batch(json_strings);

        let mut seen_file_keys = HashSet::new();
        let mut seen_txns = HashSet::new();
        let mut visitor = V1CheckpointVisitor::new(
            &mut seen_file_keys,
            true,
            vec![false; 4],
            0,
            false,
            false,
            &mut seen_txns,
        );

        visitor.visit_rows_of(batch.as_ref())?;

        // Only the first two should be included since they have different (path, DvID) keys
        let expected = vec![true, true, false, false];
        assert_eq!(visitor.selection_vector, expected);
        assert_eq!(visitor.total_file_actions, 2);
        assert_eq!(visitor.total_add_actions, 2);
        assert_eq!(visitor.total_non_file_actions, 0);

        Ok(())
    }

    #[test]
    fn test_v1_checkpoint_visitor_already_seen_non_file_actions() -> DeltaResult<()> {
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

        let mut visitor = V1CheckpointVisitor::new(
            &mut seen_file_keys,
            true,
            vec![false; 3],
            0,
            true,           // The visior has already seen a protocol action
            true,           // The visitor has already seen a metadata action
            &mut seen_txns, // Pre-populated transaction
        );

        visitor.visit_rows_of(batch.as_ref())?;

        // All actions should be skipped as they have already been seen
        let expected = vec![false, false, false];
        assert_eq!(visitor.selection_vector, expected);
        assert_eq!(visitor.total_non_file_actions, 0);
        assert_eq!(visitor.total_file_actions, 0);

        Ok(())
    }

    #[test]
    fn test_v1_checkpoint_visitor_duplicate_non_file_actions() -> DeltaResult<()> {
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
        let mut visitor = V1CheckpointVisitor::new(
            &mut seen_file_keys,
            true, // is_log_batch
            vec![false; 7],
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
        assert_eq!(visitor.total_non_file_actions, 4); // 2 txns + 1 protocol + 1 metadata
        assert_eq!(visitor.total_file_actions, 0);

        Ok(())
    }

    /// Tests the end-to-end processing of multiple batches with various action types.
    /// This tests the integration of the visitors with the main iterator function.
    /// More granular testing is performed in the visitor tests.
    #[test]
    fn test_v1_checkpoint_actions_iter_multi_batch_test() -> DeltaResult<()> {
        // Setup counters
        let total_actions_counter = Arc::new(AtomicI64::new(0));
        let total_add_actions_counter = Arc::new(AtomicI64::new(0));

        // Create first batch with protocol, metadata, and some files
        let json_strings1: StringArray = vec![
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            r#"{"metaData":{"id":"test2","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"c1\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c2\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c3\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["c1","c2"],"configuration":{},"createdTime":1670892997849}}"#,
            r#"{"add":{"path":"file1","partitionValues":{"c1":"4","c2":"c"},"size":452,"modificationTime":1670892998135,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"c3\":5},\"maxValues\":{\"c3\":5},\"nullCount\":{\"c3\":0}}"}}"#,
            r#"{"add":{"path":"file2","partitionValues":{"c1":"4","c2":"c"},"size":452,"modificationTime":1670892998135,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"c3\":5},\"maxValues\":{\"c3\":5},\"nullCount\":{\"c3\":0}}"}}"#,
        ].into();

        // Create second batch with some duplicates and new files
        let json_strings2: StringArray = vec![
            // Protocol and metadata should be skipped as duplicates
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            r#"{"metaData":{"id":"test1","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"c1\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c2\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c3\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["c1","c2"],"configuration":{},"createdTime":1670892997849}}"#,
            // New files
            r#"{"add":{"path":"file3","partitionValues":{},"size":800,"modificationTime":102,"dataChange":true}}"#,
            // Duplicate file should be skipped
            r#"{"add":{"path":"file1","partitionValues":{"c1":"4","c2":"c"},"size":452,"modificationTime":1670892998135,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"c3\":5},\"maxValues\":{\"c3\":5},\"nullCount\":{\"c3\":0}}"}}"#,            // Transaction
            r#"{"txn":{"appId":"app1","version":1,"lastUpdated":123456789}}"#
        ].into();

        // Create third batch with all duplicate actions.
        // The entire batch should be skippped as there are no selected actions to write from this batch.
        let json_strings3: StringArray = vec![
            r#"{"add":{"path":"file1","partitionValues":{"c1":"4","c2":"c"},"size":452,"modificationTime":1670892998135,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"c3\":5},\"maxValues\":{\"c3\":5},\"nullCount\":{\"c3\":0}}"}}"#,
            r#"{"add":{"path":"file2","partitionValues":{"c1":"4","c2":"c"},"size":452,"modificationTime":1670892998135,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"c3\":5},\"maxValues\":{\"c3\":5},\"nullCount\":{\"c3\":0}}"}}"#,
        ].into();

        let input_batches = vec![
            Ok((parse_json_batch(json_strings1), true)),
            Ok((parse_json_batch(json_strings2), true)),
            Ok((parse_json_batch(json_strings3), true)),
        ];

        // Run the iterator
        let results: Vec<_> = checkpoint_actions_iter(
            input_batches.into_iter(),
            total_actions_counter.clone(),
            total_add_actions_counter.clone(),
            0,
        )
        .collect::<Result<Vec<_>, _>>()?;

        // Expect two batches in results (third batch should be filtered out)"
        assert_eq!(results.len(), 2);

        // First batch should have all rows selected
        let checkpoint_data = &results[0];
        assert_eq!(
            checkpoint_data.selection_vector,
            vec![true, true, true, true]
        );

        // Second batch should have only new file and transaction selected
        let checkpoint_data = &results[1];
        assert_eq!(
            checkpoint_data.selection_vector,
            vec![false, false, true, false, true]
        );

        // 6 total actions (4 from batch1 + 2 from batch2 + 0 from batch3)
        assert_eq!(total_actions_counter.load(Ordering::Relaxed), 6);

        // 3 add actions (2 from batch1 + 1 from batch2)
        assert_eq!(total_add_actions_counter.load(Ordering::Relaxed), 3);

        Ok(())
    }
}
