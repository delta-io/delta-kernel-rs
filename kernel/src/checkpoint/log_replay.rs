use std::collections::HashSet;

use crate::actions::visitors::{CheckpointFileActionsVisitor, CheckpointNonFileActionsVisitor};
use crate::engine_data::RowVisitor;
use crate::log_replay::FileActionKey;
use crate::{DeltaResult, EngineData};

pub struct CheckpointData {
    pub data: Box<dyn EngineData>,
    pub selection_vector: Vec<bool>,
}

#[allow(unused)] // TODO: Remove once checkpoint_v1 API is implemented
struct CheckpointFileActionsLogReplayProcessor {
    /// Tracks file actions that have been seen during log replay to avoid duplicates.
    /// Contains (data file path, dv_unique_id) pairs as `FileActionKey` instances.
    seen_file_keys: HashSet<FileActionKey>,

    include_commit_batches: bool,
}

impl CheckpointFileActionsLogReplayProcessor {
    fn process_file_action_batch(
        &mut self,
        batch: Box<dyn EngineData>,
        is_log_batch: bool,
    ) -> DeltaResult<CheckpointData> {
        // Initialize selection vector with all rows un-selected
        let selection_vector = vec![false; batch.len()];
        assert_eq!(
            selection_vector.len(),
            batch.len(),
            "Initial selection vector length does not match actions length"
        );

        // Create the checkpoint visitor to process actions and update selection vector
        let mut visitor = CheckpointFileActionsVisitor {
            seen_file_keys: &mut self.seen_file_keys,
            selection_vector,
            total_add_actions: 0,
            minimum_file_retention_timestamp: 1000,
            is_log_batch,
        };
        // Process actions and let visitor update selection vector
        visitor.visit_rows_of(batch.as_ref())?;

        // TODO: Update counters

        let result = if !self.include_commit_batches && is_log_batch {
            vec![false; batch.len()]
        } else {
            // If commit batches are not included, only select rows that are not part of a commit
            visitor.selection_vector
        };

        Ok(CheckpointData {
            data: batch,
            selection_vector: result,
        })
    }
}

#[allow(unused)] // TODO: Remove once checkpoint_v1 API is implemented
impl CheckpointFileActionsLogReplayProcessor {
    pub(super) fn new(include_commit_batches: bool) -> Self {
        Self {
            seen_file_keys: Default::default(),
            include_commit_batches,
        }
    }
}

#[allow(unused)] // TODO: Remove once checkpoint_v1 API is implemented
#[derive(Default)]
struct CheckpointNonFileActionsLogReplayProcessor {
    seen_txn: HashSet<String>,
    seen_protocol: bool,
    seen_metadata: bool,
}

impl CheckpointNonFileActionsLogReplayProcessor {
    fn process_non_file_actions_batch(
        &mut self,
        batch: Box<dyn EngineData>,
        _: bool,
    ) -> DeltaResult<CheckpointData> {
        // Initialize selection vector with all rows un-selected
        let selection_vector = vec![false; batch.len()];
        assert_eq!(
            selection_vector.len(),
            batch.len(),
            "Initial selection vector length does not match actions length"
        );

        // Create the checkpoint visitor to process actions and update selection vector
        let mut visitor = CheckpointNonFileActionsVisitor {
            seen_protocol: self.seen_protocol,
            seen_metadata: self.seen_metadata,
            seen_txns: &mut self.seen_txn,
            selection_vector,
            total_actions: 0,
        };

        self.seen_metadata = visitor.seen_metadata;
        self.seen_protocol = visitor.seen_protocol;

        // Process actions and let visitor update selection vector
        visitor.visit_rows_of(batch.as_ref())?;

        // TODO: Update counters

        Ok(CheckpointData {
            data: batch,
            selection_vector: visitor.selection_vector,
        })
    }
}

// Filter iterator to only include file actions that should be written to the V2 checkpoint file.
#[allow(unused)] // TODO: Remove once checkpoint_v1 API is implemented
pub(crate) fn checkpoint_file_actions_iter(
    action_iter: impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send + 'static,
    include_commit_batches: bool,
) -> impl Iterator<Item = DeltaResult<CheckpointData>> + Send + 'static {
    let mut log_scanner = CheckpointFileActionsLogReplayProcessor::new(include_commit_batches);
    action_iter
        .map(move |action_res| {
            let (batch, is_log_batch) = action_res?;
            log_scanner.process_file_action_batch(batch, is_log_batch)
        })
        .filter(|res| {
            res.as_ref()
                .map_or(true, |data| data.selection_vector.contains(&true))
        })
}

// Filter iterator to only include non-file actions that should be written to the V2 checkpoint file.
#[allow(unused)] // TODO: Remove once checkpoint_v1 API is implemented
pub(crate) fn checkpoint_non_file_actions_iter(
    action_iter: impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send + 'static,
) -> impl Iterator<Item = DeltaResult<CheckpointData>> + Send + 'static {
    let mut log_scanner = CheckpointNonFileActionsLogReplayProcessor::default();
    action_iter
        .map(move |action_res| {
            let (batch, is_log_batch) = action_res?;
            log_scanner.process_non_file_actions_batch(batch, is_log_batch)
        })
        .filter(|res| {
            res.as_ref()
                .map_or(true, |data| data.selection_vector.contains(&true))
        })
}
