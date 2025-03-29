use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::actions::visitors::CheckpointVisitor;
use crate::engine_data::RowVisitor;
use crate::log_replay::{FileActionKey, HasSelectionVector, LogReplayProcessor};
use crate::{DeltaResult, EngineData};

pub struct CheckpointData {
    pub data: Box<dyn EngineData>,
    pub selection_vector: Vec<bool>,
}

impl HasSelectionVector for CheckpointData {
    fn has_selected_rows(&self) -> bool {
        self.selection_vector.contains(&true)
    }
}

/// `CheckpointLogReplayProcessor` is responsible for filtering actions during log
/// replay to include only those that should be included in a V1 checkpoint.
#[allow(unused)] // TODO: Remove once checkpoint_v1 API is implemented
struct CheckpointLogReplayProcessor {
    /// Tracks file actions that have been seen during log replay to avoid duplicates.
    /// Contains (data file path, dv_unique_id) pairs as `FileActionKey` instances.
    seen_file_keys: HashSet<FileActionKey>,

    /// Counter for the total number of actions processed during log replay.
    total_actions: Arc<AtomicU64>,

    /// Counter for the total number of add actions processed during log replay.
    total_add_actions: Arc<AtomicU64>,

    /// Indicates whether a protocol action has been seen in the log.
    seen_protocol: bool,

    /// Indicates whether a metadata action has been seen in the log.
    seen_metadata: bool,

    /// Set of transaction app IDs that have been processed to avoid duplicates.
    seen_txns: HashSet<String>,

    /// Minimum timestamp for file retention, used for filtering expired tombstones.
    minimum_file_retention_timestamp: i64,
}

impl LogReplayProcessor for CheckpointLogReplayProcessor {
    // Define the processing result type as a tuple of the data and selection vector
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
    /// 3. File actions are deduplicated based on path and unique ID
    /// 4. Tombstones older than `minimum_file_retention_timestamp` are excluded
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
        let mut visitor = CheckpointVisitor::new(
            &mut self.seen_file_keys,
            selection_vector,
            is_log_batch,
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

    // Get a reference to the set of seen file keys
    fn seen_file_keys(&mut self) -> &mut HashSet<FileActionKey> {
        &mut self.seen_file_keys
    }
}

#[allow(unused)] // TODO: Remove once checkpoint_v1 API is implemented
impl CheckpointLogReplayProcessor {
    pub(super) fn new(
        total_actions_counter: Arc<AtomicU64>,
        total_add_actions_counter: Arc<AtomicU64>,
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
///  Non-selected rows _must_ be ignored. The boolean flag indicates whether the record batch
///  is a log or checkpoint batch.
///
/// Note: The iterator of (engine_data, bool) tuples must be sorted by the order of the actions in
/// the log from most recent to least recent.
#[allow(unused)] // TODO: Remove once checkpoint_v1 API is implemented
pub(crate) fn checkpoint_actions_iter(
    action_iter: impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send + 'static,
    total_actions_counter: Arc<AtomicU64>,
    total_add_actions_counter: Arc<AtomicU64>,
    minimum_file_retention_timestamp: i64,
) -> impl Iterator<Item = DeltaResult<CheckpointData>> + Send + 'static {
    let mut log_scanner = CheckpointLogReplayProcessor::new(
        total_actions_counter,
        total_add_actions_counter,
        minimum_file_retention_timestamp,
    );

    CheckpointLogReplayProcessor::apply_to_iterator(log_scanner, action_iter)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    use crate::arrow::array::StringArray;
    use crate::checkpoints::log_replay::checkpoint_actions_iter;
    use crate::utils::test_utils::parse_json_batch;
    use crate::DeltaResult;

    /// Tests the end-to-end processing of multiple batches with various action types.
    /// This tests the integration of the visitors with the main iterator function.
    /// More granular testing is performed in the individual visitor tests.
    #[test]
    fn test_v1_checkpoint_actions_iter_multi_batch_integration() -> DeltaResult<()> {
        // Setup counters
        let total_actions_counter = Arc::new(AtomicU64::new(0));
        let total_add_actions_counter = Arc::new(AtomicU64::new(0));

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

        // Create third batch with all duplicate actions (should be filtered out completely)
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

        // Expect two batches in results (third batch should be filtered)"
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

        // Verify counters
        // 6 total actions (4 from batch1 + 2 from batch2 + 0 from batch3)
        assert_eq!(total_actions_counter.load(Ordering::Relaxed), 6);

        // 3 add actions (2 from batch1 + 1 from batch2)
        assert_eq!(total_add_actions_counter.load(Ordering::Relaxed), 3);

        Ok(())
    }
}
