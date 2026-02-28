use std::clone::Clone;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};

use delta_kernel_derive::internal_api;
use serde::{Deserialize, Serialize};
use tracing::info;

use super::data_skipping::DataSkippingFilter;
use super::state_info::StateInfo;
use super::{PhysicalPredicate, ScanMetadata};
use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::expressions::{
    column_expr, column_expr_ref, column_name, ColumnName, Expression, ExpressionRef, PredicateRef,
};
use crate::kernel_predicates::{DefaultKernelPredicateEvaluator, KernelPredicateEvaluator as _};
use crate::log_replay::deduplicator::{CheckpointDeduplicator, Deduplicator};
use crate::log_replay::{
    ActionsBatch, FileActionDeduplicator, FileActionKey, LogReplayProcessor,
    ParallelLogReplayProcessor,
};
use crate::log_segment::CheckpointReadInfo;
use crate::scan::Scalar;
use crate::schema::ToSchema as _;
use crate::schema::{ColumnNamesAndTypes, DataType, MapType, SchemaRef, StructField, StructType};
use crate::table_features::ColumnMappingMode;
use crate::transforms::{get_transform_expr, parse_partition_values, TransformSpec};
use crate::utils::require;
use crate::{DeltaResult, Engine, Error, ExpressionEvaluator};

/// Internal serializable state (schemas, transform spec, column mapping, etc.)
/// NOTE: This is opaque to the user - it is passed through as a blob.
#[derive(serde::Serialize, serde::Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct InternalScanState {
    logical_schema: Arc<StructType>,
    physical_schema: Arc<StructType>,
    predicate_schema: Option<Arc<StructType>>,
    transform_spec: Option<Arc<TransformSpec>>,
    column_mapping_mode: ColumnMappingMode,
    /// Physical stats schema for reading/parsing stats from checkpoint files
    physical_stats_schema: Option<SchemaRef>,
    /// Logical stats schema for the file statistics.
    logical_stats_schema: Option<SchemaRef>,
    #[serde(default)]
    skip_stats: bool,
}

/// Serializable processor state for distributed processing. This can be serialized using the
/// defualt serde serialization, or through custom serialization in the engine.
///
/// This struct contains all the information needed to reconstruct a `ScanLogReplayProcessor`
/// on remote compute nodes, enabling distributed log replay processing.
///
/// # Serialization Limitations
///
/// - **Opaque expressions**: Predicates containing [`Predicate::Opaque`] or expressions containing
///   [`Expression::Opaque`] cannot be serialized using serde. Attempting to serialize state with
///   opaque expressions will result in an error. Connectors that require opaque expression support
///   can work around this by serializing the predicate separately using their own serialization
///   mechanism, then reconstructing the processor state on the remote node.
///
/// - **Large state**: The `seen_file_keys` field can be large for tables with many commits.
///   Connectors are free to serialize this field using their own format (e.g., more compact binary
///   representations) rather than using the serde-based serialization.
///
/// [`Predicate::Opaque`]: crate::expressions::Predicate::Opaque
/// [`Expression::Opaque`]: crate::expressions::Expression::Opaque
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SerializableScanState {
    /// Optional predicate for data skipping (if provided)
    pub predicate: Option<PredicateRef>,
    /// Opaque internal state blob
    pub internal_state_blob: Vec<u8>,
    /// Set of file action keys that have already been processed.
    pub seen_file_keys: HashSet<FileActionKey>,
    /// Information about checkpoint reading for stats optimization
    pub(crate) checkpoint_info: CheckpointReadInfo,
}

/// [`ScanLogReplayProcessor`] performs log replay (processes actions) specifically for doing a table scan.
///
/// During a table scan, the processor reads batches of log actions (in reverse chronological order)
/// and performs the following steps:
///
/// - Data Skipping: Applies a predicate-based filter (via [`DataSkippingFilter`]) to quickly skip
///   files that are irrelevant for the query.
/// - Partition Pruning: Uses an optional partition filter (extracted from a physical predicate)
///   to exclude actions whose partition values do not meet the required criteria.
/// - Action Deduplication: Leverages the [`FileActionDeduplicator`] to ensure that for each unique file
///   (identified by its path and deletion vector unique ID), only the latest valid Add action is processed.
/// - Transformation: Applies a built-in transformation (`add_transform`) to convert selected Add actions
///   into [`ScanMetadata`], the intermediate format passed to the engine.
/// - Row Transform Passthrough: Any user-provided row-level transformation expressions (e.g. those derived
///   from projection or filters) are preserved and passed through to the engine, which applies them as part
///   of its scan execution logic.
///
/// As an implementation of [`LogReplayProcessor`], [`ScanLogReplayProcessor`] provides the
/// `process_actions_batch` method, which applies these steps to each batch of log actions and
/// produces a [`ScanMetadata`] result. This result includes the transformed batch, a selection
/// vector indicating which rows are valid, and any row-level transformation expressions that need
/// to be applied to the selected rows.
#[allow(rustdoc::broken_intra_doc_links, rustdoc::private_intra_doc_links)]
pub struct ScanLogReplayProcessor {
    partition_filter: Option<PredicateRef>,
    data_skipping_filter: Option<DataSkippingFilter>,
    /// Transform for log batches (commit files) - uses ParseJson for stats
    log_transform: Arc<dyn ExpressionEvaluator>,
    /// Transform for checkpoint batches - uses coalesce(stats_parsed, ParseJson) when available
    checkpoint_transform: Arc<dyn ExpressionEvaluator>,
    state_info: Arc<StateInfo>,
    /// A set of (data file path, dv_unique_id) pairs that have been seen thus
    /// far in the log. This is used to filter out files with Remove actions as
    /// well as duplicate entries in the log.
    seen_file_keys: HashSet<FileActionKey>,
    /// Skip reading file statistics.
    skip_stats: bool,
    /// Information about checkpoint reading for stats optimization
    #[allow(dead_code)]
    checkpoint_info: CheckpointReadInfo,
    metrics: Arc<ScanMetrics>,
    /// Information about checkpoint reading for stats optimization
    #[allow(dead_code)]
    checkpoint_info: CheckpointReadInfo,
}

impl ScanLogReplayProcessor {
    // These index positions correspond to the order of columns defined in
    // `selected_column_names_and_types()`
    const ADD_PATH_INDEX: usize = 0; // Position of "add.path" in getters
    const ADD_PARTITION_VALUES_INDEX: usize = 1; // Position of "add.partitionValues" in getters
    const ADD_DV_START_INDEX: usize = 2; // Start position of add deletion vector columns
    const BASE_ROW_ID_INDEX: usize = 5; // Position of add.baseRowId in getters
    const REMOVE_PATH_INDEX: usize = 6; // Position of "remove.path" in getters
    const REMOVE_DV_START_INDEX: usize = 7; // Start position of remove deletion vector columns

    /// Create a new [`ScanLogReplayProcessor`] instance
    pub(crate) fn new(
        engine: &dyn Engine,
        state_info: Arc<StateInfo>,
        checkpoint_info: CheckpointReadInfo,
        skip_stats: bool,
    ) -> DeltaResult<Self> {
        Self::new_with_seen_files(
            engine,
            state_info,
            checkpoint_info,
            Default::default(),
            skip_stats,
        )
    }

    /// Create new [`ScanLogReplayProcessor`] with pre-populated seen_file_keys.
    ///
    /// This is useful when reconstructing a processor from serialized state, where the
    /// seen_file_keys have already been computed during a previous phase of log replay.
    ///
    /// # Parameters
    /// - `engine`: Engine for creating evaluators and filters
    /// - `state_info`: StateInfo containing schemas, transforms, and predicates
    /// - `checkpoint_info`: Information about checkpoint reading for stats optimization
    /// - `seen_file_keys`: Pre-computed set of file action keys that have been seen
    /// - `skip_stats`: Skip reading file statistics
    pub(crate) fn new_with_seen_files(
        engine: &dyn Engine,
        state_info: Arc<StateInfo>,
        checkpoint_info: CheckpointReadInfo,
        seen_file_keys: HashSet<FileActionKey>,
        skip_stats: bool,
    ) -> DeltaResult<Self> {
        let CheckpointReadInfo {
            has_stats_parsed,
            checkpoint_read_schema,
        } = checkpoint_info.clone();

        // Extract the physical predicate for data skipping and partition filtering.
        // DataSkippingFilter expects Option<(PredicateRef, SchemaRef)>.
        let physical_predicate = match &state_info.physical_predicate {
            PhysicalPredicate::Some(predicate, schema) => Some((predicate.clone(), schema.clone())),
            _ => None,
        };

        // When skip_stats is enabled, use None for stats schema to produce null stats output
        let stats_schema_for_transform = if skip_stats {
            None
        } else {
            state_info.physical_stats_schema.clone()
        };

        let output_schema = scan_row_schema_with_stats_parsed(stats_schema_for_transform.clone());

        // Create data skipping filter that reads stats_parsed from the transformed batch.
        // This avoids double JSON parsing - the transform parses JSON once, then data skipping
        // reads the already-parsed stats_parsed column from the transform output.
        // Disabled when skip_stats is enabled.
        let data_skipping_filter = if skip_stats {
            None
        } else {
            state_info
                .physical_stats_schema
                .as_ref()
                .and_then(|physical_stats_schema| {
                    DataSkippingFilter::new(
                        engine,
                        // these are all cheap arc clones
                        physical_predicate.as_ref().map(|(p, _)| p.clone()),
                        physical_stats_schema.clone(),
                        output_schema.clone(),
                        column_expr_ref!("stats_parsed"),
                    )
                })
        };

        Ok(Self {
            partition_filter: physical_predicate.as_ref().map(|(p, _)| p.clone()),
            data_skipping_filter,
            // Log transform: always parse JSON (no stats_parsed in JSON commit files)
            log_transform: engine.evaluation_handler().new_expression_evaluator(
                checkpoint_read_schema.clone(),
                get_add_transform_expr(stats_schema_for_transform.clone(), false, skip_stats),
                output_schema.clone().into(),
            )?,
            // Checkpoint transform: read stats_parsed directly when available, otherwise parse JSON
            checkpoint_transform: engine.evaluation_handler().new_expression_evaluator(
                checkpoint_read_schema,
                get_add_transform_expr(stats_schema_for_transform, has_stats_parsed, skip_stats),
                output_schema.into(),
            )?,
            seen_file_keys,
            state_info,
            skip_stats,
            checkpoint_info,
            metrics: Default::default(),
            checkpoint_info,
        })
    }

    /// Get a reference to the checkpoint info.
    pub(crate) fn checkpoint_info(&self) -> &CheckpointReadInfo {
        &self.checkpoint_info
    }

    pub(crate) fn get_metrics(&self) -> &ScanMetrics {
        self.metrics.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn get_metrics_arc(&self) -> Arc<ScanMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Serialize the processor state for distributed processing.
    ///
    /// Consumes the processor and returns a `SerializableScanState` containing:
    /// - The predicate (if any) for data skipping
    /// - An opaque internal state blob (schemas, transform spec, column mapping mode)
    /// - The set of seen file keys including their deletion vector information
    ///
    /// The returned state can be used with `from_serializable_state` to reconstruct the
    /// processor on remote compute nodes.
    ///
    /// WARNING: The SerializableScanState may only be deserialized using an equal binary version
    /// of delta-kernel-rs. Using different versions for serialization and deserialization leads to
    /// undefined behaviour!
    #[internal_api]
    #[allow(unused)]
    pub(crate) fn into_serializable_state(self) -> DeltaResult<SerializableScanState> {
        let StateInfo {
            logical_schema,
            physical_schema,
            physical_predicate,
            transform_spec,
            column_mapping_mode,
            physical_stats_schema,
            logical_stats_schema,
        } = self.state_info.as_ref().clone();

        // Extract predicate from PhysicalPredicate
        let (predicate, predicate_schema) = match physical_predicate {
            PhysicalPredicate::Some(pred, schema) => (Some(pred), Some(schema)),
            _ => (None, None),
        };

        // Serialize internal state to JSON blob (schemas, transform spec, and column mapping mode)
        let internal_state = InternalScanState {
            logical_schema,
            physical_schema,
            transform_spec,
            predicate_schema,
            column_mapping_mode,
            physical_stats_schema,
            logical_stats_schema,
            skip_stats: self.skip_stats,
        };
        let internal_state_blob = serde_json::to_vec(&internal_state)
            .map_err(|e| Error::generic(format!("Failed to serialize internal state: {}", e)))?;

        Ok(SerializableScanState {
            predicate,
            internal_state_blob,
            seen_file_keys: self.seen_file_keys,
            checkpoint_info: CheckpointReadInfo::without_stats_parsed(),
        })
    }

    /// Reconstruct a processor from serialized state.
    ///
    /// Creates a new processor with the provided state. All fields (partition_filter,
    /// data_skipping_filter, add_transform, and seen_file_keys) are reconstructed from
    /// the serialized state and engine.
    ///
    /// # Parameters
    /// - `engine`: Engine for creating evaluators and filters
    /// - `state`: The serialized state containing predicate, internal state blob, and seen file keys
    ///
    /// # Returns
    /// A new `ScanLogReplayProcessor` wrapped in an Arc.
    ///
    #[internal_api]
    #[allow(unused)]
    pub(crate) fn from_serializable_state(
        engine: &dyn Engine,
        state: SerializableScanState,
    ) -> DeltaResult<Self> {
        // Deserialize internal state from json
        let internal_state: InternalScanState =
            serde_json::from_slice(&state.internal_state_blob).map_err(Error::MalformedJson)?;

        // Reconstruct PhysicalPredicate from predicate and predicate schema
        let physical_predicate = match state.predicate {
            Some(predicate) => {
                let Some(predicate_schema) = internal_state.predicate_schema else {
                    return Err(Error::generic(
                        "Invalid serialized internal state. Expected predicate schema.",
                    ));
                };
                PhysicalPredicate::Some(predicate, predicate_schema)
            }
            None => PhysicalPredicate::None,
        };

        let state_info = Arc::new(StateInfo {
            logical_schema: internal_state.logical_schema,
            physical_schema: internal_state.physical_schema,
            physical_predicate,
            transform_spec: internal_state.transform_spec,
            column_mapping_mode: internal_state.column_mapping_mode,
            physical_stats_schema: internal_state.physical_stats_schema,
            logical_stats_schema: internal_state.logical_stats_schema,
        });

        Self::new_with_seen_files(
            engine,
            state_info,
            state.checkpoint_info,
            state.seen_file_keys,
            internal_state.skip_stats,
        )
    }
}
/// Metrics collected from [`ScanLogReplayProcessor`]
#[internal_api]
pub(crate) struct ScanMetrics {
    num_adds: AtomicU64,
    num_removes: AtomicU64,
    num_non_file_actions: AtomicU64,
    hash_set_size: AtomicUsize,
    data_skipping_filtered: AtomicU64,
    partition_pruning_filtered: AtomicU64,
    // Timing metrics (in nanoseconds)
    dedup_visitor_time_ns: AtomicU64,
    data_skipping_time_ns: AtomicU64,
    partition_pruning_time_ns: AtomicU64,
    // If set, log metrics on drop with this message
    log_on_drop_message: Mutex<Option<String>>,
}

impl Default for ScanMetrics {
    fn default() -> Self {
        Self {
            num_adds: AtomicU64::new(0),
            num_removes: AtomicU64::new(0),
            num_non_file_actions: AtomicU64::new(0),
            hash_set_size: AtomicUsize::new(0),
            data_skipping_filtered: AtomicU64::new(0),
            partition_pruning_filtered: AtomicU64::new(0),
            dedup_visitor_time_ns: AtomicU64::new(0),
            data_skipping_time_ns: AtomicU64::new(0),
            partition_pruning_time_ns: AtomicU64::new(0),
            log_on_drop_message: Mutex::new(None),
        }
    }
}

impl ScanMetrics {
    pub(crate) fn reset_counters(&self) {
        // NOTE: We do not reset hash set size because that never decreases. All subsequent uses of
        // the processor will reuse the same hashset.
        self.num_adds.store(0, Ordering::SeqCst);
        self.num_removes.store(0, Ordering::SeqCst);
        self.num_non_file_actions.store(0, Ordering::SeqCst);
        self.data_skipping_filtered.store(0, Ordering::SeqCst);
        self.partition_pruning_filtered.store(0, Ordering::SeqCst);
        self.dedup_visitor_time_ns.store(0, Ordering::SeqCst);
        self.data_skipping_time_ns.store(0, Ordering::SeqCst);
        self.partition_pruning_time_ns.store(0, Ordering::SeqCst);
    }
    pub(crate) fn incr_adds(&self) {
        self.num_adds.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn incr_removes(&self) {
        self.num_removes.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn incr_partition_pruning_filtered(&self) {
        self.partition_pruning_filtered
            .fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn incr_non_file_actions(&self) {
        self.num_non_file_actions.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn add_data_skipping_filtered(&self, value: u64) {
        self.data_skipping_filtered
            .fetch_add(value, Ordering::Relaxed);
    }
    pub(crate) fn set_hash_set(&self, value: usize) {
        self.hash_set_size.fetch_max(value, Ordering::SeqCst);
    }

    pub(crate) fn add_dedup_visitor_time_ns(&self, duration_ns: u64) {
        self.dedup_visitor_time_ns
            .fetch_add(duration_ns, Ordering::Relaxed);
    }

    pub(crate) fn add_data_skipping_time_ns(&self, duration_ns: u64) {
        self.data_skipping_time_ns
            .fetch_add(duration_ns, Ordering::Relaxed);
    }

    pub(crate) fn add_partition_pruning_time_ns(&self, duration_ns: u64) {
        self.partition_pruning_time_ns
            .fetch_add(duration_ns, Ordering::Relaxed);
    }

    pub(crate) fn log_with_message(&self, message: impl AsRef<str>) {
        let num_adds = self.num_adds.load(Ordering::Relaxed);
        let num_removes = self.num_removes.load(Ordering::Relaxed);
        let num_non_file_actions = self.num_non_file_actions.load(Ordering::Relaxed);
        let hash_set_size = self.hash_set_size.load(Ordering::Relaxed);
        let data_skipping_filtered = self.data_skipping_filtered.load(Ordering::Relaxed);
        let partition_pruning_filtered = self.partition_pruning_filtered.load(Ordering::Relaxed);
        let dedup_visitor_time_ms = self.dedup_visitor_time_ns.load(Ordering::Relaxed) / 1_000_000;
        let data_skipping_time_ms = self.data_skipping_time_ns.load(Ordering::Relaxed) / 1_000_000;
        let partition_pruning_time_ms =
            self.partition_pruning_time_ns.load(Ordering::Relaxed) / 1_000_000;
        info!(
            num_adds,
            num_removes,
            num_non_file_actions,
            hash_set_size,
            data_skipping_filtered,
            partition_pruning_filtered,
            dedup_visitor_time_ms,
            data_skipping_time_ms,
            partition_pruning_time_ms,
            "{}",
            message.as_ref()
        );
    }

    /// Set the message to log when these metrics are dropped.
    pub(crate) fn set_log_on_drop(&self, message: impl Into<String>) {
        if let Ok(mut guard) = self.log_on_drop_message.lock() {
            *guard = Some(message.into());
        }
    }

    /// Clear the log-on-drop message (disables logging on drop).
    pub(crate) fn clear_log_on_drop(&self) {
        if let Ok(mut guard) = self.log_on_drop_message.lock() {
            *guard = None;
        }
    }
}

impl Drop for ScanMetrics {
    fn drop(&mut self) {
        if let Ok(guard) = self.log_on_drop_message.lock() {
            if let Some(message) = guard.as_ref() {
                self.log_with_message(message);
            }
        }
    }
}

/// A visitor that deduplicates a stream of add and remove actions into a stream of valid adds. Log
/// replay visits actions newest-first, so once we've seen a file action for a given (path, dvId)
/// pair, we should ignore all subsequent (older) actions for that same (path, dvId) pair. If the
/// first action for a given file is a remove, then that file does not show up in the result at all.
struct AddRemoveDedupVisitor<D: Deduplicator> {
    deduplicator: D,
    selection_vector: Vec<bool>,
    state_info: Arc<StateInfo>,
    partition_filter: Option<PredicateRef>,
    row_transform_exprs: Vec<Option<ExpressionRef>>,
    metrics: Arc<ScanMetrics>,
}

impl<D: Deduplicator> AddRemoveDedupVisitor<D> {
    fn new(
        deduplicator: D,
        selection_vector: Vec<bool>,
        state_info: Arc<StateInfo>,
        partition_filter: Option<PredicateRef>,
        metrics: Arc<ScanMetrics>,
    ) -> AddRemoveDedupVisitor<D> {
        AddRemoveDedupVisitor {
            deduplicator,
            selection_vector,
            state_info,
            partition_filter,
            row_transform_exprs: Vec::new(),
            metrics,
        }
    }

    fn is_file_partition_pruned(
        &self,
        partition_values: &HashMap<usize, (String, Scalar)>,
    ) -> bool {
        if partition_values.is_empty() {
            return false;
        }
        let Some(partition_filter) = &self.partition_filter else {
            return false;
        };
        let partition_values: HashMap<_, _> = partition_values
            .values()
            .map(|(k, v)| (ColumnName::new([k]), v.clone()))
            .collect();
        let evaluator = DefaultKernelPredicateEvaluator::from(partition_values);
        evaluator.eval_sql_where(partition_filter) == Some(false)
    }

    /// True if this row contains an Add action that should survive log replay. Skip it if the row
    /// is not an Add action, or the file has already been seen previously.
    fn is_valid_add<'a>(&mut self, i: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<bool> {
        // When processing file actions, we extract path and deletion vector information based on action type:
        // - For Add actions: path is at index 0, followed by DV fields at indexes 2-4
        // - For Remove actions (in log batches only): path is at index 5, followed by DV fields at indexes 6-8
        // The file extraction logic selects the appropriate indexes based on whether we found a valid path.
        // Remove getters are not included when visiting a non-log batch (checkpoint batch), so do
        // not try to extract remove actions in that case.
        let Some((file_key, is_add)) = self.deduplicator.extract_file_action(
            i,
            getters,
            !self.deduplicator.is_log_batch(), // skip_removes. true if this is a checkpoint batch
        )?
        else {
            self.metrics.incr_non_file_actions();
            return Ok(false);
        };

        if is_add {
            self.metrics.incr_adds()
        } else {
            self.metrics.incr_removes()
        };

        // Apply partition pruning (to adds only) before deduplication, so that we don't waste memory
        // tracking pruned files. Removes don't get pruned and we'll still have to track them.
        //
        // WARNING: It's not safe to partition-prune removes (just like it's not safe to data skip
        // removes), because they are needed to suppress earlier incompatible adds we might
        // encounter if the table's schema was replaced after the most recent checkpoint.
        let partition_values = match &self.state_info.transform_spec {
            Some(transform) if is_add => {
                let start = std::time::Instant::now();

                let partition_values = getters[ScanLogReplayProcessor::ADD_PARTITION_VALUES_INDEX]
                    .get(i, "add.partitionValues")?;
                let partition_values = parse_partition_values(
                    &self.state_info.logical_schema,
                    transform,
                    &partition_values,
                    self.state_info.column_mapping_mode,
                )?;

                let is_pruned = self.is_file_partition_pruned(&partition_values);

                let elapsed_ns = start.elapsed().as_nanos() as u64;
                self.metrics.add_partition_pruning_time_ns(elapsed_ns);

                if is_pruned {
                    self.metrics.incr_partition_pruning_filtered();
                    return Ok(false);
                }

                partition_values
            }
            _ => Default::default(),
        };

        // Check both adds and removes (skipping already-seen), but only transform and return adds
        if self.deduplicator.check_and_record_seen(file_key) || !is_add {
            return Ok(false);
        }
        let base_row_id: Option<i64> =
            getters[ScanLogReplayProcessor::BASE_ROW_ID_INDEX].get_opt(i, "add.baseRowId")?;
        let transform = self
            .state_info
            .transform_spec
            .as_ref()
            .map(|transform| {
                get_transform_expr(
                    transform,
                    partition_values,
                    &self.state_info.physical_schema,
                    base_row_id,
                )
            })
            .transpose()?;
        if transform.is_some() {
            // fill in any needed `None`s for previous rows
            self.row_transform_exprs.resize_with(i, Default::default);
            self.row_transform_exprs.push(transform);
        }
        Ok(true)
    }
}

impl<D: Deduplicator> RowVisitor for AddRemoveDedupVisitor<D> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        // NOTE: The visitor assumes a schema with adds first and removes optionally afterward.
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            const STRING: DataType = DataType::STRING;
            const INTEGER: DataType = DataType::INTEGER;
            const LONG: DataType = DataType::LONG;
            let ss_map: DataType = MapType::new(STRING, STRING, true).into();
            let types_and_names = vec![
                (STRING, column_name!("add.path")),
                (ss_map, column_name!("add.partitionValues")),
                (STRING, column_name!("add.deletionVector.storageType")),
                (STRING, column_name!("add.deletionVector.pathOrInlineDv")),
                (INTEGER, column_name!("add.deletionVector.offset")),
                (LONG, column_name!("add.baseRowId")),
                (STRING, column_name!("remove.path")),
                (STRING, column_name!("remove.deletionVector.storageType")),
                (STRING, column_name!("remove.deletionVector.pathOrInlineDv")),
                (INTEGER, column_name!("remove.deletionVector.offset")),
            ];
            let (types, names) = types_and_names.into_iter().unzip();
            (names, types).into()
        });
        let (names, types) = NAMES_AND_TYPES.as_ref();
        if self.deduplicator.is_log_batch() {
            (names, types)
        } else {
            // All checkpoint actions are already reconciled and Remove actions in checkpoint files
            // only serve as tombstones for vacuum jobs. So we only need to examine the adds here.
            (&names[..6], &types[..6])
        }
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        let start = std::time::Instant::now();

        let is_log_batch = self.deduplicator.is_log_batch();
        let expected_getters = if is_log_batch { 10 } else { 6 };
        require!(
            getters.len() == expected_getters,
            Error::InternalError(format!(
                "Wrong number of AddRemoveDedupVisitor getters: {}",
                getters.len()
            ))
        );

        for i in 0..row_count {
            if self.selection_vector[i] {
                self.selection_vector[i] = self.is_valid_add(i, getters)?;
            }
        }

        let elapsed_ns = start.elapsed().as_nanos() as u64;
        self.metrics.add_dedup_visitor_time_ns(elapsed_ns);

        Ok(())
    }
}

pub(crate) static FILE_CONSTANT_VALUES_NAME: &str = "fileConstantValues";
pub(crate) static BASE_ROW_ID_NAME: &str = "baseRowId";
pub(crate) static DEFAULT_ROW_COMMIT_VERSION_NAME: &str = "defaultRowCommitVersion";
pub(crate) static CLUSTERING_PROVIDER_NAME: &str = "clusteringProvider";
pub(crate) static TAGS_NAME: &str = "tags";

// NB: If you update this schema, ensure you update the comment describing it in the doc comment
// for `scan_row_schema` in scan/mod.rs! You'll also need to update ScanFileVisitor as the
// indexes will be off, and [`get_add_transform_expr`] below to match it.
pub(crate) static SCAN_ROW_SCHEMA: LazyLock<Arc<StructType>> = LazyLock::new(|| {
    // Note that fields projected out of a nullable struct must be nullable
    let partition_values = MapType::new(DataType::STRING, DataType::STRING, true);
    let file_constant_values = StructType::new_unchecked([
        StructField::nullable("partitionValues", partition_values),
        StructField::nullable(BASE_ROW_ID_NAME, DataType::LONG),
        StructField::nullable(DEFAULT_ROW_COMMIT_VERSION_NAME, DataType::LONG),
        StructField::nullable(
            "tags",
            MapType::new(
                DataType::STRING,
                DataType::STRING,
                /*valueContainsNull*/ true,
            ),
        ),
        StructField::nullable(CLUSTERING_PROVIDER_NAME, DataType::STRING),
    ]);
    Arc::new(StructType::new_unchecked([
        StructField::nullable("path", DataType::STRING),
        StructField::nullable("size", DataType::LONG),
        StructField::nullable("modificationTime", DataType::LONG),
        StructField::nullable("stats", DataType::STRING),
        StructField::nullable("deletionVector", DeletionVectorDescriptor::to_schema()),
        StructField::nullable(FILE_CONSTANT_VALUES_NAME, file_constant_values),
    ]))
});

/// Build the scan row schema with optional stats_parsed column.
///
/// When `stats_schema` is provided, adds a `stats_parsed` struct column with that schema.
fn scan_row_schema_with_stats_parsed(stats_schema: Option<SchemaRef>) -> SchemaRef {
    match stats_schema {
        Some(schema) => {
            let mut fields: Vec<StructField> = SCAN_ROW_SCHEMA.fields().cloned().collect();
            fields.push(StructField::nullable(
                "stats_parsed",
                schema.as_ref().clone(),
            ));
            Arc::new(StructType::new_unchecked(fields))
        }
        None => SCAN_ROW_SCHEMA.clone(),
    }
}

/// Build the add transform expression with optional stats parsing.
///
/// # Parameters
/// - `physical_stats_schema`: Schema for parsing stats from JSON and for output (physical column
///   names), or None if stats should not be included in output.
/// - `has_stats_parsed`: Whether checkpoint has pre-parsed stats_parsed column.
/// - `skip_stats`: When true, replaces the stats column with a null literal, avoiding reads of the
///   raw stats JSON string from checkpoint parquet files.
///
/// The transform includes `stats_parsed` only when `physical_stats_schema` is Some.
/// Stats are output using physical column names. Engines can use `Scan::logical_stats_schema()`
/// to map physical names back to logical names when column mapping is enabled.
fn get_add_transform_expr(
    physical_stats_schema: Option<SchemaRef>,
    has_stats_parsed: bool,
    skip_stats: bool,
) -> ExpressionRef {
    let stats_expr = if skip_stats {
        Arc::new(Expression::Literal(Scalar::Null(DataType::STRING)))
    } else {
        column_expr_ref!("add.stats")
    };
    let mut fields = vec![
        column_expr_ref!("add.path"),
        column_expr_ref!("add.size"),
        column_expr_ref!("add.modificationTime"),
        stats_expr,
        column_expr_ref!("add.deletionVector"),
        Arc::new(Expression::Struct(vec![
            column_expr_ref!("add.partitionValues"),
            column_expr_ref!("add.baseRowId"),
            column_expr_ref!("add.defaultRowCommitVersion"),
            column_expr_ref!("add.tags"),
            column_expr_ref!("add.clusteringProvider"),
        ])),
    ];

    // Add stats_parsed when stats output is requested (using physical column names)
    if let Some(stats_schema) = physical_stats_schema {
        let stats_parsed_expr = if has_stats_parsed {
            // Checkpoint has stats_parsed column - read directly
            column_expr!("add.stats_parsed")
        } else {
            // No stats_parsed available (JSON log files) - parse JSON
            Expression::parse_json(column_expr!("add.stats"), stats_schema)
        };
        fields.push(Arc::new(stats_parsed_expr));
    }

    Arc::new(Expression::Struct(fields))
}

// TODO: Move this to transaction/mod.rs once `scan_metadata_from` is pub, as this is used for
// deletion vector update transformations.
#[allow(unused)]
pub(crate) fn get_scan_metadata_transform_expr() -> ExpressionRef {
    use crate::expressions::column_expr_ref;
    static EXPR: LazyLock<ExpressionRef> = LazyLock::new(|| {
        Arc::new(Expression::Struct(vec![Arc::new(Expression::Struct(
            vec![
                column_expr_ref!("path"),
                column_expr_ref!("fileConstantValues.partitionValues"),
                column_expr_ref!("size"),
                column_expr_ref!("modificationTime"),
                column_expr_ref!("stats"),
                column_expr_ref!("fileConstantValues.tags"),
                column_expr_ref!("deletionVector"),
                column_expr_ref!("fileConstantValues.baseRowId"),
                column_expr_ref!("fileConstantValues.defaultRowCommitVersion"),
                column_expr_ref!("fileConstantValues.clusteringProvider"),
            ],
        ))]))
    });
    EXPR.clone()
}

impl ParallelLogReplayProcessor for ScanLogReplayProcessor {
    type Output = <ScanLogReplayProcessor as LogReplayProcessor>::Output;

    // WARNING: This function performs all the same operations as [`<ScanLogReplayProcessor as
    // LogReplayProcessor>::process_actions_batch`]! (See trait impl block below) Any changes
    // performed to this function probably also need to be applied to the other copy of the
    // function. The copy exists because [`LogReplayProcessor`] requires a `&mut self`, while
    // [`ParallelLogReplayProcessor`] requires `&self`. Presently, the different in mutabilities
    // cannot easily be unified.
    fn process_actions_batch(&self, actions_batch: ActionsBatch) -> DeltaResult<Self::Output> {
        let ActionsBatch {
            actions,
            is_log_batch,
        } = actions_batch;
        require!(
            !is_log_batch,
            Error::generic("Parallel checkpoint processor may only be applied to checkpoint files")
        );

        // Step 1: Apply transform FIRST (parses JSON once, outputs stats_parsed).
        // This is done before data skipping so we can read the already-parsed stats.
        // We use the checkpoint_transform because we checked above that we're reading a checkpoint.
        let transformed = self.checkpoint_transform.evaluate(actions.as_ref())?;
        debug_assert_eq!(transformed.len(), actions.len());
        require!(
            transformed.len() == actions.len(),
            Error::internal_error(format!(
                "checkpoint transform output length {} != actions length {}",
                transformed.len(),
                actions.len()
            ))
        );

        // Step 2: Build selection vector from TRANSFORMED batch (reads stats_parsed directly)
        // This avoids double JSON parsing - the transform already parsed the stats.
        let selection_vector =
            self.build_selection_vector(transformed.as_ref(), self.metrics.as_ref())?;
        debug_assert_eq!(selection_vector.len(), actions.len());
        require!(
            selection_vector.len() == actions.len(),
            Error::internal_error(format!(
                "selection vector length {} != actions length {}",
                selection_vector.len(),
                actions.len()
            ))
        );

        // Step 3: Run deduplication visitor on RAW batch (needs add.path, remove.path, etc.)
        let deduplicator = CheckpointDeduplicator::try_new(
            &self.seen_file_keys,
            Self::ADD_PATH_INDEX,
            Self::ADD_DV_START_INDEX,
        )?;
        let mut visitor = AddRemoveDedupVisitor::new(
            deduplicator,
            selection_vector,
            self.state_info.clone(),
            self.partition_filter.clone(),
            self.metrics.clone(),
        );
        visitor.visit_rows_of(actions.as_ref())?;

        // Step 4: Return transformed batch with updated selection vector
        let scan_metadata = ScanMetadata::try_new(
            transformed,
            visitor.selection_vector,
            visitor.row_transform_exprs,
        )?;
        self.metrics.set_hash_set(self.seen_file_keys.len());
        Ok(scan_metadata)
    }
}

impl LogReplayProcessor for ScanLogReplayProcessor {
    type Output = ScanMetadata;

    // WARNING: This function performs all the same operations as [`<ScanLogReplayProcessor as
    // ParallelLogReplayProcessor>::process_actions_batch`]! Any changes performed to this function
    // probably also need to be applied to the other copy. The copy exists because
    // [`LogReplayProcessor`] requires a `&mut self`, while [`ParallelLogReplayProcessor`] requires
    // `&self`. Presently, the different in mutabilities cannot easily be unified.
    fn process_actions_batch(&mut self, actions_batch: ActionsBatch) -> DeltaResult<Self::Output> {
        let ActionsBatch {
            actions,
            is_log_batch,
        } = actions_batch;

        // Step 1: Apply transform FIRST (parses JSON once, outputs stats_parsed)
        // Use the correct transform based on batch type:
        // - Log batches: use ParseJson (no stats_parsed in JSON commit files)
        // - Checkpoint batches: use coalesce(stats_parsed, ParseJson) when available
        let transform = if is_log_batch {
            &self.log_transform
        } else {
            &self.checkpoint_transform
        };
        let transformed = transform.evaluate(actions.as_ref())?;
        debug_assert_eq!(transformed.len(), actions.len());
        require!(
            transformed.len() == actions.len(),
            Error::internal_error(format!(
                "transform output length {} != actions length {}",
                transformed.len(),
                actions.len()
            ))
        );

        // Step 2: Build selection vector from TRANSFORMED batch (reads stats_parsed directly)
        // This avoids double JSON parsing - the transform already parsed the stats.
        let selection_vector =
            self.build_selection_vector(transformed.as_ref(), self.metrics.as_ref())?;
        debug_assert_eq!(selection_vector.len(), actions.len());
        require!(
            selection_vector.len() == actions.len(),
            Error::internal_error(format!(
                "selection vector length {} != actions length {}",
                selection_vector.len(),
                actions.len()
            ))
        );

        // Step 3: Run deduplication visitor on RAW batch (needs add.path, remove.path, etc.)
        let deduplicator = FileActionDeduplicator::new(
            &mut self.seen_file_keys,
            is_log_batch,
            Self::ADD_PATH_INDEX,
            Self::REMOVE_PATH_INDEX,
            Self::ADD_DV_START_INDEX,
            Self::REMOVE_DV_START_INDEX,
        );
        let mut visitor = AddRemoveDedupVisitor::new(
            deduplicator,
            selection_vector,
            self.state_info.clone(),
            self.partition_filter.clone(),
            self.metrics.clone(),
        );
        visitor.visit_rows_of(actions.as_ref())?;

        // Step 4: Return transformed batch with updated selection vector
        let scan_metadata = ScanMetadata::try_new(
            transformed,
            visitor.selection_vector,
            visitor.row_transform_exprs,
        )?;
        self.metrics.set_hash_set(self.seen_file_keys.len());
        Ok(scan_metadata)
    }

    fn data_skipping_filter(&self) -> Option<&DataSkippingFilter> {
        self.data_skipping_filter.as_ref()
    }
}

/// Given an iterator of [`ActionsBatch`]s (batches of actions read from the log) and a predicate,
/// returns an iterator of [`ScanMetadata`]s (which includes the files to be scanned as
/// [`FilteredEngineData`] and transforms that must be applied to correctly read the data). Each row
/// that is selected in the returned `engine_data` _must_ be processed to complete the scan.
/// Non-selected rows _must_ be ignored.
///
/// When `skip_stats` is true, file statistics are not read from checkpoint parquet files and
/// data skipping is disabled.
///
/// Note: The iterator of [`ActionsBatch`]s ('action_iter' parameter) must be sorted by the order of
/// the actions in the log from most recent to least recent.
pub(crate) fn scan_action_iter(
    engine: &dyn Engine,
    action_iter: impl Iterator<Item = DeltaResult<ActionsBatch>>,
    state_info: Arc<StateInfo>,
    checkpoint_info: CheckpointReadInfo,
    skip_stats: bool,
) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanMetadata>>> {
    Ok(
        ScanLogReplayProcessor::new(engine, state_info, checkpoint_info, skip_stats)?
            .process_actions_iter(action_iter),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use crate::actions::get_commit_schema;
    use crate::engine::sync::SyncEngine;
    use crate::expressions::{
        BinaryExpressionOp, OpaquePredicateOp, Predicate, Scalar, ScalarExpressionEvaluator,
    };
    use crate::kernel_predicates::{
        DirectDataSkippingPredicateEvaluator, DirectPredicateEvaluator,
        IndirectDataSkippingPredicateEvaluator,
    };
    use crate::log_replay::ActionsBatch;
    use crate::log_segment::CheckpointReadInfo;
    use crate::scan::state::ScanFile;
    use crate::scan::state_info::tests::{
        assert_transform_spec, get_simple_state_info, get_state_info, ROW_TRACKING_FEATURES,
    };
    use crate::scan::state_info::StateInfo;
    use crate::scan::test_utils::{
        add_batch_for_row_id, add_batch_simple, add_batch_with_partition_col,
        add_batch_with_remove, run_with_validate_callback,
    };
    use crate::scan::PhysicalPredicate;
    use crate::schema::MetadataColumnSpec;
    use crate::schema::{DataType, SchemaRef, StructField, StructType};
    use crate::table_features::ColumnMappingMode;
    use crate::utils::test_utils::assert_result_error_with_message;
    use crate::DeltaResult;
    use crate::Expression as Expr;
    use crate::ExpressionRef;

    use super::{
        scan_action_iter, InternalScanState, ScanLogReplayProcessor, SerializableScanState,
    };
    use crate::log_replay::LogReplayProcessor;

    fn test_checkpoint_info() -> CheckpointReadInfo {
        CheckpointReadInfo::without_stats_parsed()
    }

    /// A minimal opaque predicate op for testing serialization behavior
    #[derive(Debug, PartialEq)]
    struct OpaqueTestOp(String);

    impl OpaquePredicateOp for OpaqueTestOp {
        fn name(&self) -> &str {
            &self.0
        }

        fn eval_pred_scalar(
            &self,
            _eval_expr: &ScalarExpressionEvaluator<'_>,
            _evaluator: &DirectPredicateEvaluator<'_>,
            _exprs: &[Expr],
            _inverted: bool,
        ) -> DeltaResult<Option<bool>> {
            unimplemented!()
        }

        fn eval_as_data_skipping_predicate(
            &self,
            _predicate_evaluator: &DirectDataSkippingPredicateEvaluator<'_>,
            _exprs: &[Expr],
            _inverted: bool,
        ) -> Option<bool> {
            unimplemented!()
        }

        fn as_data_skipping_predicate(
            &self,
            _predicate_evaluator: &IndirectDataSkippingPredicateEvaluator<'_>,
            _exprs: &[Expr],
            _inverted: bool,
        ) -> Option<Predicate> {
            unimplemented!()
        }
    }

    // dv-info is more complex to validate, we validate that works in the test for visit_scan_files
    // in state.rs
    fn validate_simple(_: &mut (), scan_file: ScanFile) {
        assert_eq!(
            scan_file.path,
            "part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet"
        );
        assert_eq!(scan_file.size, 635);
        assert!(scan_file.stats.is_some());
        assert_eq!(scan_file.stats.as_ref().unwrap().num_records, 10);
        assert_eq!(
            scan_file.partition_values.get("date"),
            Some(&"2017-12-10".to_string())
        );
        assert_eq!(scan_file.partition_values.get("non-existent"), None);
    }

    #[test]
    fn test_scan_action_iter() {
        run_with_validate_callback(
            vec![add_batch_simple(get_commit_schema().clone())],
            None, // not testing schema
            None, // not testing transform
            &[true, false],
            (),
            validate_simple,
        );
    }

    #[test]
    fn test_scan_action_iter_with_remove() {
        run_with_validate_callback(
            vec![add_batch_with_remove(get_commit_schema().clone())],
            None, // not testing schema
            None, // not testing transform
            &[false, false, true, false],
            (),
            validate_simple,
        );
    }

    #[test]
    fn test_no_transforms() {
        let batch = vec![add_batch_simple(get_commit_schema().clone())];
        let logical_schema = Arc::new(StructType::new_unchecked(vec![]));
        let state_info = Arc::new(StateInfo {
            logical_schema: logical_schema.clone(),
            physical_schema: logical_schema.clone(),
            physical_predicate: PhysicalPredicate::None,
            transform_spec: None,
            column_mapping_mode: ColumnMappingMode::None,
            physical_stats_schema: None,
            logical_stats_schema: None,
        });
        let iter = scan_action_iter(
            &SyncEngine::new(),
            batch
                .into_iter()
                .map(|batch| Ok(ActionsBatch::new(batch as _, true))),
            state_info,
            test_checkpoint_info(),
            false,
        )
        .unwrap();
        for res in iter {
            let scan_metadata = res.unwrap();
            assert!(
                scan_metadata.scan_file_transforms.is_empty(),
                "Should have no transforms"
            );
        }
    }

    #[test]
    fn test_simple_transform() {
        let schema: SchemaRef = Arc::new(StructType::new_unchecked([
            StructField::new("value", DataType::INTEGER, true),
            StructField::new("date", DataType::DATE, true),
        ]));
        let partition_cols = vec!["date".to_string()];
        let state_info = get_simple_state_info(schema, partition_cols).unwrap();
        let batch = vec![add_batch_with_partition_col()];
        let iter = scan_action_iter(
            &SyncEngine::new(),
            batch
                .into_iter()
                .map(|batch| Ok(ActionsBatch::new(batch as _, true))),
            Arc::new(state_info),
            test_checkpoint_info(),
            false,
        )
        .unwrap();

        fn validate_transform(transform: Option<&ExpressionRef>, expected_date_offset: i32) {
            assert!(transform.is_some());
            let Expr::Transform(transform) = transform.unwrap().as_ref() else {
                panic!("Transform should always be a Transform expr");
            };

            // With sparse transforms, we expect only one insertion for the partition column
            assert!(transform.prepended_fields.is_empty());
            let mut field_transforms = transform.field_transforms.iter();
            let (field_name, field_transform) = field_transforms.next().unwrap();
            assert_eq!(field_name, "value");
            assert!(!field_transform.is_replace);
            let [expr] = &field_transform.exprs[..] else {
                panic!("Expected a single insertion");
            };
            let Expr::Literal(Scalar::Date(date_offset)) = expr.as_ref() else {
                panic!("Expected a literal date");
            };
            assert_eq!(*date_offset, expected_date_offset);
            assert!(field_transforms.next().is_none());
        }

        for res in iter {
            let scan_metadata = res.unwrap();
            let transforms = scan_metadata.scan_file_transforms;
            // in this case we have a metadata action first and protocol 3rd, so we expect 4 items,
            // the first and 3rd being a `None`
            assert_eq!(transforms.len(), 4, "Should have 4 transforms");
            assert!(transforms[0].is_none(), "transform at [0] should be None");
            assert!(transforms[2].is_none(), "transform at [2] should be None");
            validate_transform(transforms[1].as_ref(), 17511);
            validate_transform(transforms[3].as_ref(), 17510);
        }
    }

    #[test]
    fn test_row_id_transform() {
        let schema: SchemaRef = Arc::new(StructType::new_unchecked([StructField::new(
            "value",
            DataType::INTEGER,
            true,
        )]));
        let state_info = get_state_info(
            schema.clone(),
            vec![],
            None,
            ROW_TRACKING_FEATURES,
            [
                ("delta.enableRowTracking", "true"),
                (
                    "delta.rowTracking.materializedRowIdColumnName",
                    "row_id_col",
                ),
                (
                    "delta.rowTracking.materializedRowCommitVersionColumnName",
                    "row_commit_version_col",
                ),
            ]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
            vec![("row_id", MetadataColumnSpec::RowId)],
        )
        .unwrap();

        let transform_spec = state_info.transform_spec.as_ref().unwrap();
        assert_transform_spec(
            transform_spec,
            false,
            "row_id_col",
            "row_indexes_for_row_id_0",
        );

        let batch = vec![add_batch_for_row_id(get_commit_schema().clone())];
        let iter = scan_action_iter(
            &SyncEngine::new(),
            batch
                .into_iter()
                .map(|batch| Ok(ActionsBatch::new(batch as _, true))),
            Arc::new(state_info),
            test_checkpoint_info(),
            false,
        )
        .unwrap();

        for res in iter {
            let scan_metadata = res.unwrap();
            let transforms = scan_metadata.scan_file_transforms;
            assert_eq!(transforms.len(), 1, "Should have 1 transform");
            if let Some(Expr::Transform(transform_expr)) = transforms[0].as_ref().map(Arc::as_ref) {
                assert!(transform_expr.input_path.is_none());
                let row_id_transform = transform_expr
                    .field_transforms
                    .get("row_id_col")
                    .expect("Should have row_id_col transform");
                assert!(row_id_transform.is_replace);
                assert_eq!(row_id_transform.exprs.len(), 1);
                let expr = &row_id_transform.exprs[0];
                let expeceted_expr = Arc::new(Expr::coalesce([
                    Expr::column(["row_id_col"]),
                    Expr::binary(
                        BinaryExpressionOp::Plus,
                        Expr::literal(42i64),
                        Expr::column(["row_indexes_for_row_id_0"]),
                    ),
                ]));
                assert_eq!(expr, &expeceted_expr);
            } else {
                panic!("Should have been a transform expression");
            }
        }
    }

    #[test]
    fn test_serialization_basic_state_and_dv_dropping() {
        // Test basic StateInfo preservation and FileActionKey preservation
        let engine = SyncEngine::new();
        let schema: SchemaRef = Arc::new(StructType::new_unchecked([
            StructField::new("id", DataType::INTEGER, true),
            StructField::new("value", DataType::STRING, true),
        ]));
        let checkpoint_info = test_checkpoint_info();
        let mut processor = ScanLogReplayProcessor::new(
            &engine,
            Arc::new(get_simple_state_info(schema.clone(), vec![]).unwrap()),
            checkpoint_info.clone(),
            false,
        )
        .unwrap();

        // Add file keys with and without DV info
        let key1 = crate::log_replay::FileActionKey::new("file1.parquet", None);
        let key2 = crate::log_replay::FileActionKey::new("file2.parquet", Some("dv-1".to_string()));
        let key3 = crate::log_replay::FileActionKey::new("file3.parquet", Some("dv-2".to_string()));
        processor.seen_file_keys.insert(key1.clone());
        processor.seen_file_keys.insert(key2.clone());
        processor.seen_file_keys.insert(key3.clone());

        let state_info = processor.state_info.clone();
        let deserialized = ScanLogReplayProcessor::from_serializable_state(
            &engine,
            processor.into_serializable_state().unwrap(),
        )
        .unwrap();

        // Verify StateInfo fields preserved
        assert_eq!(
            deserialized.state_info.logical_schema,
            state_info.logical_schema
        );
        assert_eq!(
            deserialized.state_info.physical_schema,
            state_info.physical_schema
        );
        assert_eq!(
            deserialized.state_info.column_mapping_mode,
            state_info.column_mapping_mode
        );

        // Verify all file keys are preserved with their DV info
        assert_eq!(deserialized.seen_file_keys.len(), 3);
        assert!(deserialized.seen_file_keys.contains(&key1));
        assert!(deserialized.seen_file_keys.contains(&key2));
        assert!(deserialized.seen_file_keys.contains(&key3));
    }

    #[test]
    fn test_serialization_with_predicate() {
        // Test that PhysicalPredicate and predicate schema are preserved
        let engine = SyncEngine::new();
        let schema: SchemaRef = Arc::new(StructType::new_unchecked([
            StructField::new("id", DataType::INTEGER, true),
            StructField::new("value", DataType::STRING, true),
        ]));
        let predicate = Arc::new(crate::expressions::Predicate::eq(
            Expr::column(["id"]),
            Expr::literal(10i32),
        ));
        let state_info = Arc::new(
            get_state_info(
                schema.clone(),
                vec![],
                Some(predicate.clone()),
                &[], // no table features
                HashMap::new(),
                vec![],
            )
            .unwrap(),
        );
        let original_pred_schema = match &state_info.physical_predicate {
            PhysicalPredicate::Some(_, s) => s.clone(),
            _ => panic!("Expected predicate"),
        };
        let checkpoint_info = test_checkpoint_info();
        let processor = ScanLogReplayProcessor::new(
            &engine,
            state_info.clone(),
            checkpoint_info.clone(),
            false,
        )
        .unwrap();
        let deserialized = ScanLogReplayProcessor::from_serializable_state(
            &engine,
            processor.into_serializable_state().unwrap(),
        )
        .unwrap();

        match &deserialized.state_info.physical_predicate {
            PhysicalPredicate::Some(pred, pred_schema) => {
                assert_eq!(pred.as_ref(), predicate.as_ref());
                assert_eq!(pred_schema.as_ref(), original_pred_schema.as_ref());
            }
            _ => panic!("Expected PhysicalPredicate::Some"),
        }
    }

    #[test]
    fn test_serialization_with_transforms() {
        // Test transform_spec preservation (partition columns + row tracking)
        let engine = SyncEngine::new();
        let schema: SchemaRef = Arc::new(StructType::new_unchecked([
            StructField::new("value", DataType::INTEGER, true),
            StructField::new("date", DataType::DATE, true),
        ]));
        let state_info = Arc::new(
            get_state_info(
                schema,
                vec!["date".to_string()],
                None,
                ROW_TRACKING_FEATURES,
                [
                    ("delta.enableRowTracking", "true"),
                    (
                        "delta.rowTracking.materializedRowIdColumnName",
                        "row_id_col",
                    ),
                    (
                        "delta.rowTracking.materializedRowCommitVersionColumnName",
                        "row_commit_version_col",
                    ),
                ]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
                vec![("row_id", MetadataColumnSpec::RowId)],
            )
            .unwrap(),
        );
        let original_transform = state_info.transform_spec.clone();
        assert!(original_transform.is_some());
        let checkpoint_info = test_checkpoint_info();
        let processor = ScanLogReplayProcessor::new(
            &engine,
            state_info.clone(),
            checkpoint_info.clone(),
            false,
        )
        .unwrap();
        let deserialized = ScanLogReplayProcessor::from_serializable_state(
            &engine,
            processor.into_serializable_state().unwrap(),
        )
        .unwrap();
        assert_eq!(deserialized.state_info.transform_spec, original_transform);
    }

    #[test]
    fn test_serialization_column_mapping_modes() {
        // Test that different ColumnMappingMode values are preserved
        let engine = SyncEngine::new();
        for mode in [
            ColumnMappingMode::None,
            ColumnMappingMode::Id,
            ColumnMappingMode::Name,
        ] {
            let schema: SchemaRef = Arc::new(StructType::new_unchecked([StructField::new(
                "id",
                DataType::INTEGER,
                true,
            )]));
            let state_info = Arc::new(StateInfo {
                logical_schema: schema.clone(),
                physical_schema: schema,
                physical_predicate: PhysicalPredicate::None,
                transform_spec: None,
                column_mapping_mode: mode,
                physical_stats_schema: None,
                logical_stats_schema: None,
            });
            let checkpoint_info = test_checkpoint_info();
            let processor =
                ScanLogReplayProcessor::new(&engine, state_info, checkpoint_info.clone(), false)
                    .unwrap();
            let deserialized = ScanLogReplayProcessor::from_serializable_state(
                &engine,
                processor.into_serializable_state().unwrap(),
            )
            .unwrap();
            assert_eq!(deserialized.state_info.column_mapping_mode, mode);
        }
    }

    #[test]
    fn test_serialization_edge_cases() {
        // Test edge cases: empty seen_file_keys, no predicate, no transform_spec
        let engine = SyncEngine::new();
        let checkpoint_info = test_checkpoint_info();
        let schema: SchemaRef = Arc::new(StructType::new_unchecked([StructField::new(
            "id",
            DataType::INTEGER,
            true,
        )]));
        let state_info = Arc::new(StateInfo {
            logical_schema: schema.clone(),
            physical_schema: schema,
            physical_predicate: PhysicalPredicate::None,
            transform_spec: None,
            column_mapping_mode: ColumnMappingMode::None,
            physical_stats_schema: None,
            logical_stats_schema: None,
        });
        let processor =
            ScanLogReplayProcessor::new(&engine, state_info, checkpoint_info.clone(), false)
                .unwrap();
        let serialized = processor.into_serializable_state().unwrap();
        assert!(serialized.predicate.is_none());
        let deserialized =
            ScanLogReplayProcessor::from_serializable_state(&engine, serialized).unwrap();
        assert_eq!(deserialized.seen_file_keys.len(), 0);
        assert!(deserialized.state_info.transform_spec.is_none());
    }

    #[test]
    fn test_serialization_invalid_json() {
        // Test that invalid JSON blobs are properly rejected
        let engine = SyncEngine::new();
        let checkpoint_info = test_checkpoint_info();
        let invalid_state = SerializableScanState {
            predicate: None,
            internal_state_blob: vec![0, 1, 2, 3, 255], // Invalid JSON
            seen_file_keys: HashSet::new(),
            checkpoint_info,
        };
        assert!(ScanLogReplayProcessor::from_serializable_state(&engine, invalid_state).is_err());
    }

    #[test]
    fn test_serialization_missing_predicate_schema() {
        // Test that missing predicate_schema when predicate exists is detected
        let engine = SyncEngine::new();
        let schema: SchemaRef = Arc::new(StructType::new_unchecked([StructField::new(
            "id",
            DataType::INTEGER,
            true,
        )]));
        let checkpoint_info = test_checkpoint_info();
        let invalid_internal_state = InternalScanState {
            logical_schema: schema.clone(),
            physical_schema: schema,
            predicate_schema: None, // Missing!
            transform_spec: None,
            column_mapping_mode: ColumnMappingMode::None,
            physical_stats_schema: None,
            logical_stats_schema: None,
            skip_stats: false,
        };
        let predicate = Arc::new(crate::expressions::Predicate::column(["id"]));
        let invalid_blob = serde_json::to_vec(&invalid_internal_state).unwrap();
        let invalid_state = SerializableScanState {
            predicate: Some(predicate), // Predicate exists but schema is None
            internal_state_blob: invalid_blob,
            seen_file_keys: HashSet::new(),
            checkpoint_info,
        };
        let result = ScanLogReplayProcessor::from_serializable_state(&engine, invalid_state);
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("predicate schema"));
        }
    }

    #[test]
    fn deserialize_internal_state_with_extry_fields_fails() {
        let schema: SchemaRef = Arc::new(StructType::new_unchecked([StructField::new(
            "id",
            DataType::INTEGER,
            true,
        )]));
        let invalid_internal_state = InternalScanState {
            logical_schema: schema.clone(),
            physical_schema: schema,
            predicate_schema: None,
            transform_spec: None,
            column_mapping_mode: ColumnMappingMode::None,
            physical_stats_schema: None,
            logical_stats_schema: None,
            skip_stats: false,
        };
        let blob = serde_json::to_string(&invalid_internal_state).unwrap();
        let mut obj: serde_json::Value = serde_json::from_str(&blob).unwrap();
        obj["new_field"] = serde_json::json!("my_new_value");
        let invalid_blob = obj.to_string();

        let res: Result<InternalScanState, _> = serde_json::from_str(&invalid_blob);
        assert_result_error_with_message(res, "unknown field");
    }

    #[test]
    fn deserialize_serializable_scan_state_with_extra_fields_fails() {
        let state = SerializableScanState {
            predicate: None,
            internal_state_blob: vec![],
            seen_file_keys: HashSet::new(),
            checkpoint_info: test_checkpoint_info(),
        };
        let blob = serde_json::to_string(&state).unwrap();
        let mut obj: serde_json::Value = serde_json::from_str(&blob).unwrap();
        obj["new_field"] = serde_json::json!("my_new_value");
        let invalid_blob = obj.to_string();

        let res: Result<SerializableScanState, _> = serde_json::from_str(&invalid_blob);
        assert_result_error_with_message(res, "unknown field");
    }

    #[test]
    fn serializng_scan_state_with_opaque_predicate_fails() {
        // Opaque predicates cannot be serialized. Connectors requiring opaque expression support
        // must serialize the predicate separately using their own mechanism.

        // Create an opaque predicate
        let opaque_predicate = Arc::new(Predicate::opaque(OpaqueTestOp("test_op".to_string()), []));

        // Directly create a SerializableScanState with the opaque predicate
        let state = SerializableScanState {
            predicate: Some(opaque_predicate),
            internal_state_blob: vec![],
            seen_file_keys: HashSet::new(),
            checkpoint_info: test_checkpoint_info(),
        };

        // Serialization should fail because opaque expressions cannot be serialized
        let result = serde_json::to_string(&state);
        assert_result_error_with_message(result, "Cannot serialize an Opaque Predicate");
    }

    #[test]
    fn test_scan_action_iter_with_skip_stats() {
        let batch = vec![add_batch_simple(get_commit_schema().clone())];
        let schema: SchemaRef = Arc::new(StructType::new_unchecked([
            StructField::new("value", DataType::INTEGER, true),
            StructField::new("date", DataType::DATE, true),
        ]));
        let state_info = get_simple_state_info(schema, vec!["date".to_string()]).unwrap();

        let iter = scan_action_iter(
            &SyncEngine::new(),
            batch
                .into_iter()
                .map(|batch| Ok(ActionsBatch::new(batch as _, true))),
            Arc::new(state_info),
            test_checkpoint_info(),
            true,
        )
        .unwrap();

        let mut found_add = false;
        for res in iter {
            let scan_metadata = res.unwrap();
            scan_metadata
                .visit_scan_files((), |_: &mut (), scan_file: ScanFile| {
                    assert!(scan_file.stats.is_none());
                })
                .unwrap();
            found_add = true;
        }
        assert!(found_add);
    }

    #[test]
    fn test_scan_metrics_counters() {
        use super::ScanMetrics;
        use std::sync::atomic::Ordering;

        let metrics = ScanMetrics::default();

        // Test initial state - all counters should be zero
        assert_eq!(metrics.num_adds.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.num_removes.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.num_non_file_actions.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.data_skipping_filtered.load(Ordering::Relaxed), 0);
        assert_eq!(
            metrics.partition_pruning_filtered.load(Ordering::Relaxed),
            0
        );
        assert_eq!(metrics.dedup_visitor_time_ns.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.data_skipping_time_ns.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.partition_pruning_time_ns.load(Ordering::Relaxed), 0);

        // Test counter increments
        metrics.incr_adds();
        metrics.incr_adds();
        metrics.incr_adds();
        assert_eq!(metrics.num_adds.load(Ordering::Relaxed), 3);

        metrics.incr_removes();
        metrics.incr_removes();
        assert_eq!(metrics.num_removes.load(Ordering::Relaxed), 2);

        metrics.incr_non_file_actions();
        assert_eq!(metrics.num_non_file_actions.load(Ordering::Relaxed), 1);

        metrics.incr_partition_pruning_filtered();
        metrics.incr_partition_pruning_filtered();
        assert_eq!(
            metrics.partition_pruning_filtered.load(Ordering::Relaxed),
            2
        );

        // Test data skipping filtered (can add multiple at once)
        metrics.add_data_skipping_filtered(10);
        metrics.add_data_skipping_filtered(5);
        assert_eq!(metrics.data_skipping_filtered.load(Ordering::Relaxed), 15);

        // Test hash set size (uses fetch_max, so only largest value is kept)
        metrics.set_hash_set(100);
        metrics.set_hash_set(50); // Should not change it
        metrics.set_hash_set(200); // Should update to 200
        assert_eq!(metrics.hash_set_size.load(Ordering::Relaxed), 200);

        // Test timing metrics accumulation
        metrics.add_dedup_visitor_time_ns(1_000_000); // 1ms
        metrics.add_dedup_visitor_time_ns(2_000_000); // 2ms
        assert_eq!(
            metrics.dedup_visitor_time_ns.load(Ordering::Relaxed),
            3_000_000
        );

        metrics.add_data_skipping_time_ns(5_000_000); // 5ms
        metrics.add_data_skipping_time_ns(3_000_000); // 3ms
        assert_eq!(
            metrics.data_skipping_time_ns.load(Ordering::Relaxed),
            8_000_000
        );

        metrics.add_partition_pruning_time_ns(10_000_000); // 10ms
        metrics.add_partition_pruning_time_ns(15_000_000); // 15ms
        assert_eq!(
            metrics.partition_pruning_time_ns.load(Ordering::Relaxed),
            25_000_000
        );

        // Test reset_counters
        metrics.reset_counters();
        assert_eq!(metrics.num_adds.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.num_removes.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.num_non_file_actions.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.data_skipping_filtered.load(Ordering::Relaxed), 0);
        assert_eq!(
            metrics.partition_pruning_filtered.load(Ordering::Relaxed),
            0
        );
        assert_eq!(metrics.dedup_visitor_time_ns.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.data_skipping_time_ns.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.partition_pruning_time_ns.load(Ordering::Relaxed), 0);
        // Hash set size should NOT be reset
        assert_eq!(metrics.hash_set_size.load(Ordering::Relaxed), 200);
    }

    #[test]
    fn test_scan_metrics_with_real_scan() {
        use std::sync::atomic::Ordering;

        // Test with add_batch_with_remove which contains:
        // - 1 remove action
        // - 1 add action
        // - 2 non-file actions (metaData, protocol)
        let batch = vec![add_batch_with_remove(get_commit_schema().clone())];
        let schema: SchemaRef = Arc::new(StructType::new_unchecked([StructField::new(
            "value",
            DataType::INTEGER,
            true,
        )]));
        let state_info = get_simple_state_info(schema, vec![]).unwrap();

        let engine = SyncEngine::new();
        let state_info_arc = Arc::new(state_info);
        let processor =
            ScanLogReplayProcessor::new(&engine, state_info_arc, test_checkpoint_info(), false)
                .unwrap();

        // Get metrics Arc before moving processor
        let metrics = processor.get_metrics_arc();

        // Process batches (process_actions_iter consumes processor)
        let _results: Vec<_> = processor
            .process_actions_iter(
                batch
                    .into_iter()
                    .map(|batch| Ok(ActionsBatch::new(batch as _, true))),
            )
            .collect::<DeltaResult<_>>()
            .unwrap();

        // Verify exact counter values based on test data
        // add_batch_with_remove contains: 2 adds, 1 remove, 1 metaData (non-file action)
        assert_eq!(
            metrics.num_adds.load(Ordering::Relaxed),
            2,
            "Expected exactly 2 add actions"
        );
        assert_eq!(
            metrics.num_removes.load(Ordering::Relaxed),
            1,
            "Expected exactly 1 remove action"
        );
        assert_eq!(
            metrics.num_non_file_actions.load(Ordering::Relaxed),
            1,
            "Expected exactly 1 non-file action (metaData)"
        );

        // Timing should have accumulated (dedup visitor was called)
        assert!(
            metrics.dedup_visitor_time_ns.load(Ordering::Relaxed) > 0,
            "Expected dedup timing to be non-zero"
        );

        // No data skipping or partition pruning in this test
        assert_eq!(
            metrics.data_skipping_filtered.load(Ordering::Relaxed),
            0,
            "Expected no data skipping filtered"
        );
        assert_eq!(
            metrics.partition_pruning_filtered.load(Ordering::Relaxed),
            0,
            "Expected no partition pruning filtered"
        );
    }
}
