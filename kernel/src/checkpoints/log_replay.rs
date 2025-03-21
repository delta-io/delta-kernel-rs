use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::actions::visitors::{CheckpointFileActionsVisitor, CheckpointNonFileActionsVisitor};
use crate::engine_data::RowVisitor;
use crate::scan::log_replay::FileActionKey;
use crate::{DeltaResult, EngineData};

/// `V1CheckpointLogReplayScanner` is responsible for filtering actions during log
/// replay to include only those that should be included in a V1 checkpoint.
#[allow(unused)] // TODO: Remove once checkpoint_v1 API is implemented
struct V1CheckpointLogReplayScanner {
    /// Tracks file actions that have been seen during log replay to avoid duplicates.
    /// Contains (data file path, dv_unique_id) pairs as `FileActionKey` instances.
    seen_file_keys: HashSet<FileActionKey>,

    /// Counter for the total number of actions processed during log replay.
    total_actions: Arc<AtomicUsize>,

    /// Counter for the total number of add actions processed during log replay.
    total_add_actions: Arc<AtomicUsize>,

    /// Indicates whether a protocol action has been seen in the log.
    seen_protocol: bool,

    /// Indicates whether a metadata action has been seen in the log.
    seen_metadata: bool,

    /// Set of transaction app IDs that have been processed to avoid duplicates.
    seen_txns: HashSet<String>,

    /// Minimum timestamp for file retention, used for filtering expired tombstones.
    minimum_file_retention_timestamp: i64,
}

#[allow(unused)] // TODO: Remove once checkpoint_v1 API is implemented
impl V1CheckpointLogReplayScanner {
    pub(super) fn new(
        total_actions_counter: Arc<AtomicUsize>,
        total_add_actions_counter: Arc<AtomicUsize>,
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

    /// Iterates over actions and filters them for inclusion in a V1 checkpoint.
    ///
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
    pub(super) fn filter_v1_checkpoint_actions(
        &mut self,
        actions: Box<dyn EngineData>,
        is_log_batch: bool,
    ) -> DeltaResult<(Box<dyn EngineData>, Vec<bool>)> {
        // Initialize selection vector with all rows un-selected
        let mut selection_vector = vec![false; actions.len()];
        assert_eq!(
            selection_vector.len(),
            actions.len(),
            "Initial selection vector length does not match actions length"
        );

        // Create the non file actions visitor to process non file actions and update selection vector
        let mut non_file_actions_visitor = CheckpointNonFileActionsVisitor {
            seen_protocol: &mut self.seen_protocol,
            seen_metadata: &mut self.seen_metadata,
            seen_txns: &mut self.seen_txns,
            selection_vector: &mut selection_vector,
            total_actions: 0,
        };

        // Process actions and let visitor update selection vector
        non_file_actions_visitor.visit_rows_of(actions.as_ref())?;

        // Update shared counters with non-file action counts from this batch
        self.total_actions
            .fetch_add(non_file_actions_visitor.total_actions, Ordering::Relaxed);

        // Create the file actions visitor to process file actions and update selection vector
        let mut file_actions_visitor = CheckpointFileActionsVisitor {
            seen_file_keys: &mut self.seen_file_keys,
            is_log_batch,
            selection_vector: &mut selection_vector,
            total_actions: 0,
            total_add_actions: 0,
            minimum_file_retention_timestamp: self.minimum_file_retention_timestamp,
        };

        // Process actions and let visitor update selection vector
        file_actions_visitor.visit_rows_of(actions.as_ref())?;

        // Update shared counters with file action counts from this batch
        self.total_actions
            .fetch_add(file_actions_visitor.total_actions, Ordering::Relaxed);
        self.total_add_actions
            .fetch_add(file_actions_visitor.total_add_actions, Ordering::Relaxed);

        Ok((actions, selection_vector))
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
pub(crate) fn v1_checkpoint_actions_iter(
    action_iter: impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send + 'static,
    total_actions_counter: Arc<AtomicUsize>,
    total_add_actions_counter: Arc<AtomicUsize>,
    minimum_file_retention_timestamp: i64,
) -> impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, Vec<bool>)>> + Send + 'static {
    let mut log_scanner = V1CheckpointLogReplayScanner::new(
        total_actions_counter,
        total_add_actions_counter,
        minimum_file_retention_timestamp,
    );

    action_iter
        .map(move |action_res| {
            let (batch, is_log_batch) = action_res?;
            log_scanner.filter_v1_checkpoint_actions(batch, is_log_batch)
        })
        // Only yield batches that have at least one selected row
        .filter(|res| res.as_ref().map_or(true, |(_, sv)| sv.contains(&true)))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use crate::arrow::array::StringArray;
    use crate::checkpoints::log_replay::v1_checkpoint_actions_iter;
    use crate::utils::test_utils::parse_json_batch;
    use crate::DeltaResult;

    /// Tests the end-to-end processing of multiple batches with various action types.
    /// This tests the integration of the visitors with the main iterator function.
    /// More granular testing is performed in the individual visitor tests.
    #[test]
    fn test_v1_checkpoint_actions_iter_multi_batch_integration() -> DeltaResult<()> {
        // Setup counters
        let total_actions_counter = Arc::new(AtomicUsize::new(0));
        let total_add_actions_counter = Arc::new(AtomicUsize::new(0));

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
        let results: Vec<_> = v1_checkpoint_actions_iter(
            input_batches.into_iter(),
            total_actions_counter.clone(),
            total_add_actions_counter.clone(),
            0,
        )
        .collect::<Result<Vec<_>, _>>()?;

        // Expect two batches in results (third batch should be filtered)"
        assert_eq!(results.len(), 2);

        // First batch should have all rows selected
        let (_, selection_vector1) = &results[0];
        assert_eq!(selection_vector1, &vec![true, true, true, true]);

        // Second batch should have only new file and transaction selected
        let (_, selection_vector2) = &results[1];
        assert_eq!(selection_vector2, &vec![false, false, true, false, true]);

        // Verify counters
        // 6 total actions (4 from batch1 + 2 from batch2 + 0 from batch3)
        assert_eq!(total_actions_counter.load(Ordering::Relaxed), 6);

        // 3 add actions (2 from batch1 + 1 from batch2)
        assert_eq!(total_add_actions_counter.load(Ordering::Relaxed), 3);

        Ok(())
    }
}
