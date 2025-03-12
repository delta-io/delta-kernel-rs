use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};

use crate::actions::visitors::{CheckpointsVisitor, FileActionsVisitor, NonFileActionsVisitor};
use crate::engine_data::{RowVisitor, TypedGetData as _};
use crate::scan::log_replay::FileActionKey;
use crate::{DeltaResult, EngineData};

/// `LogReplayScanner` is responsible for processing actions during log replay.
/// It maintains state about what actions have been seen and provides methods to filter
/// action batches differently to be written to different type of checkpoint files
/// for checkpoint generation.
struct LogReplayScanner {
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

    /// Set of transaction IDs that have been processed to avoid duplicates.
    seen_txns: HashSet<String>,

    /// Minimum timestamp for file retention, used for filtering old file actions.
    minimum_file_retention_timestamp: i64,
}

impl LogReplayScanner {
    /// Creates a new `LogReplayScanner` instance.
    ///
    /// # Parameters
    /// * `total_actions_counter` - Shared counter for tracking the total number of actions processed
    /// * `total_add_actions_counter` - Shared counter for tracking the total number of add actions processed
    /// * `minimum_file_retention_timestamp` - Minimum timestamp for file retention
    ///
    /// # Returns
    /// A new `LogReplayScanner` instance with initialized state.
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

    /// Processes a V1 checkpoint batch and produces a selection vector.
    ///
    /// This method processes a batch of actions for a V1 checkpoint by determining which
    /// actions should be included in the checkpoint file.
    pub(super) fn process_v1_checkpoint_batch(
        &mut self,
        actions: Box<dyn EngineData>,
        is_log_batch: bool,
    ) -> DeltaResult<(Box<dyn EngineData>, Vec<bool>)> {
        // Initialize selection vector with all rows selected
        let selection_vector = vec![true; actions.len()];
        assert_eq!(
            selection_vector.len(),
            actions.len(),
            "Initial selection vector length does not match actions length"
        );

        // Create visitor to process actions and update selection vector
        let mut visitor = CheckpointsVisitor {
            seen_file_keys: &mut self.seen_file_keys,
            selection_vector,
            is_log_batch,
            seen_protocol: self.seen_protocol,
            seen_metadata: self.seen_metadata,
            seen_txns: &mut self.seen_txns,
            total_actions: 0,
            total_add_actions: 0,
            minimum_file_retention_timestamp: self.minimum_file_retention_timestamp,
        };

        // Process actions and let visitor update selection vector
        visitor.visit_rows_of(actions.as_ref())?;
        let selection_vector = visitor.selection_vector;

        // Update shared counters with action counts from this batch
        self.total_actions
            .fetch_add(visitor.total_actions, Ordering::Relaxed);
        self.total_add_actions
            .fetch_add(visitor.total_add_actions, Ordering::Relaxed);

        Ok((actions, selection_vector))
    }

    /// Processes a sidecar batch and produces a selection vector.
    ///
    /// This method processes a batch of actions for sidecar files, determining which file
    /// actions should be included based on action deduplication logic.
    pub(super) fn process_sidecar_batch(
        &mut self,
        actions: Box<dyn EngineData>,
        is_log_batch: bool,
    ) -> DeltaResult<(Box<dyn EngineData>, Vec<bool>)> {
        // Initialize selection vector with all rows selected
        let selection_vector = vec![true; actions.len()];
        assert_eq!(
            selection_vector.len(),
            actions.len(),
            "Initial selection vector length does not match actions length"
        );

        // Create visitor to process only file actions
        let mut visitor = FileActionsVisitor {
            seen_file_keys: &mut self.seen_file_keys,
            selection_vector,
            is_log_batch,
            total_actions: 0,
            total_add_actions: 0,
            minimum_file_retention_timestamp: self.minimum_file_retention_timestamp,
        };

        // Process actions and let visitor update selection vector
        visitor.visit_rows_of(actions.as_ref())?;
        let selection_vector = visitor.selection_vector;

        // Update shared counters with action counts from this batch
        self.total_actions
            .fetch_add(visitor.total_actions, Ordering::Relaxed);
        self.total_add_actions
            .fetch_add(visitor.total_add_actions, Ordering::Relaxed);

        Ok((actions, selection_vector))
    }

    /// Processes a V2 checkpoint batch and produces a selection vector.
    ///
    /// This method processes a batch of actions for a V2 checkpoint, determining which
    /// non-file actions should be included in the top-level checkpoint.
    pub(super) fn process_v2_checkpoint_batch(
        &mut self,
        actions: Box<dyn EngineData>,
        _is_log_batch: bool,
    ) -> DeltaResult<(Box<dyn EngineData>, Vec<bool>)> {
        // Initialize selection vector with all rows selected
        let selection_vector = vec![true; actions.len()];
        assert_eq!(
            selection_vector.len(),
            actions.len(),
            "Initial selection vector length does not match actions length"
        );

        // Create visitor to process non-file actions only
        let mut visitor = NonFileActionsVisitor {
            seen_protocol: self.seen_protocol,
            seen_metadata: self.seen_metadata,
            seen_txns: &mut self.seen_txns,
            selection_vector,
            total_actions: 0,
        };

        // Process actions and let visitor update selection vector
        visitor.visit_rows_of(actions.as_ref())?;
        let selection_vector = visitor.selection_vector;

        // Update shared counter with action count from this batch
        self.total_actions
            .fetch_add(visitor.total_actions, Ordering::Relaxed);

        Ok((actions, selection_vector))
    }
}

/// Creates an iterator that processes Delta log actions for V1 checkpoints.
///
/// This function takes an iterator of action batches and returns an iterator that yields
/// processed batches with selection vectors indicating which rows should be written to
/// the V1 checkpoint.
pub(crate) fn v1_checkpoint_actions_iter(
    action_iter: impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send + 'static,
    total_actions_counter: Arc<AtomicUsize>,
    total_add_actions_counter: Arc<AtomicUsize>,
    minimum_file_retention_timestamp: i64,
) -> impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, Vec<bool>)>> + Send + 'static {
    let mut log_scanner = LogReplayScanner::new(
        total_actions_counter,
        total_add_actions_counter,
        minimum_file_retention_timestamp,
    );

    action_iter
        .map(move |action_res| {
            let (batch, is_log_batch) = action_res?;
            log_scanner.process_v1_checkpoint_batch(batch, is_log_batch)
        })
        // Only yield batches that have at least one selected row
        .filter(|res| res.as_ref().map_or(true, |(_, sv)| sv.contains(&true)))
}

/// Creates an iterator that processes Delta log actions for sidecars.
///
/// This function takes an iterator of action batches and returns an iterator that yields
/// processed batches with selection vectors indicating which rows should be written to
/// sidecars.
pub(crate) fn sidecar_actions_iter(
    action_iter: impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send + 'static,
    total_actions_counter: Arc<AtomicUsize>,
    total_add_actions_counter: Arc<AtomicUsize>,
    minimum_file_retention_timestamp: i64,
) -> impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, Vec<bool>)>> + Send + 'static {
    let mut log_scanner = LogReplayScanner::new(
        total_actions_counter,
        total_add_actions_counter,
        minimum_file_retention_timestamp,
    );

    action_iter
        .map(move |action_res| {
            let (batch, is_log_batch) = action_res?;
            log_scanner.process_sidecar_batch(batch, is_log_batch)
        })
        // Only yield batches that have at least one selected row
        .filter(|res| res.as_ref().map_or(true, |(_, sv)| sv.contains(&true)))
}

/// Creates an iterator that processes Delta log actions for V2 checkpoints.
///
/// This function takes an iterator of action batches and returns an iterator that yields
/// processed batches with selection vectors indicating which rows should be written to
/// the top-level V2 checkpoint file.
pub(crate) fn v2_checkpoint_actions_iter(
    action_iter: impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send + 'static,
    total_actions_counter: Arc<AtomicUsize>,
) -> impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, Vec<bool>)>> + Send + 'static {
    // Create scanner with dummy add_actions counter since it's not used for V2 checkpoints
    let mut log_scanner =
        LogReplayScanner::new(total_actions_counter, Arc::new(AtomicUsize::new(0)), 0);

    action_iter
        .map(move |action_res| {
            let (batch, is_log_batch) = action_res?;
            log_scanner.process_v2_checkpoint_batch(batch, is_log_batch)
        })
        // Only yield batches that have at least one selected row
        .filter(|res| res.as_ref().map_or(true, |(_, sv)| sv.contains(&true)))
}
