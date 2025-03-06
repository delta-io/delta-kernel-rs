use std::clone::Clone;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};

use itertools::Itertools;
use tracing::debug;

use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::actions::visitors::CheckpointsVisitor;
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::expressions::{column_expr, column_name, ColumnName, Expression, ExpressionRef};
use crate::scan::log_replay::FileActionKey;
use crate::schema::{ColumnNamesAndTypes, DataType, MapType, SchemaRef, StructField, StructType};
use crate::utils::require;
use crate::{DeltaResult, Engine, EngineData, Error, ExpressionEvaluator};

/// The subset of file action fields that uniquely identifies it in the log, used for deduplication
/// of adds and removes during log replay.

pub(super) struct LogReplayScanner {
    /// A set of (data file path, dv_unique_id) pairs that have been seen thus
    /// far in the log. This is used to reconcile file actions during log replay.
    seen_file_keys: HashSet<FileActionKey>,
    total_actions: Arc<AtomicUsize>,
    total_add_actions: Arc<AtomicUsize>,
}
impl LogReplayScanner {
    /// Processes a checkpoint batch and produces a selection vector.
    ///
    /// The returned selection vector flags each row that must be written to the checkpoint.
    pub(super) fn process_checkpoint_batch(
        &mut self,
        actions: Box<dyn EngineData>,
        is_log_batch: bool,
    ) -> DeltaResult<(Box<dyn EngineData>, Vec<bool>)> {
        // Build initial selection vector of the same length as rows and ensure lengths match.
        let selection_vector = vec![true; actions.len()];
        assert_eq!(
            selection_vector.len(),
            actions.len(),
            "Initial selection vector length does not match actions length"
        );

        let mut visitor = CheckpointsVisitor {
            seen_file_keys: &mut self.seen_file_keys,
            selection_vector,
            is_log_batch,
            seen_protocol: false,
            seen_metadata: false,
            seen_txns: Default::default(),
            total_actions: 0,
            total_add_actions: 0,
        };

        // Process the actions and let the visitor update the selection vector.
        visitor.visit_rows_of(actions.as_ref())?;
        let selection_vector = visitor.selection_vector;

        // Update atomic counters.
        self.total_actions
            .fetch_add(visitor.total_actions, Ordering::Relaxed);
        self.total_add_actions
            .fetch_add(visitor.total_add_actions, Ordering::Relaxed);

        Ok((actions, selection_vector))
    }

    /// Create a new [`LogReplayScanner`] instance
    pub(super) fn new(
        total_actions_counter: Arc<AtomicUsize>,
        total_add_actions_counter: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            seen_file_keys: Default::default(),
            total_actions: total_actions_counter,
            total_add_actions: total_add_actions_counter,
        }
    }
}

/// Given an iterator of (engine_data, bool) tuples and a predicate, returns an iterator of
/// `(engine_data, selection_vec)`. Each row that is selected in the returned `engine_data` _must_
/// be written to the checkpoint to complete the table's state. Non-selected rows _must_ be ignored.
/// The boolean flag indicates whether the record batch is a log or checkpoint batch.
pub(crate) fn checkpoint_actions_iter(
    action_iter: impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send + 'static,
    total_actions_counter: Arc<AtomicUsize>,
    total_add_actions_counter: Arc<AtomicUsize>,
) -> impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, Vec<bool>)>> + Send + 'static {
    let mut log_scanner = LogReplayScanner::new(total_actions_counter, total_add_actions_counter);

    action_iter
        .map(move |action_res| {
            let (batch, is_log_batch) = action_res?;
            log_scanner.process_checkpoint_batch(batch, is_log_batch)
        })
        .filter(|res| res.as_ref().map_or(true, |(_, sv)| sv.contains(&true)))
}
