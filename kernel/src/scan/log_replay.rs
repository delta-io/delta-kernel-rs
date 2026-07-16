use std::clone::Clone;
use std::collections::HashSet;
use std::sync::{Arc, LazyLock};

use delta_kernel_derive::internal_api;
use serde::{Deserialize, Serialize};

use super::data_skipping::DataSkippingFilter;
use super::metrics::ScanMetrics;
use super::state_info::StateInfo;
use super::{PhysicalPredicate, ScanMetadata};
use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::expressions::{
    column_expr, column_expr_ref, column_name, ColumnName, Expression, ExpressionRef, Predicate,
    PredicateRef, UnaryExpressionOp,
};
use crate::log_replay::deduplicator::{CheckpointDeduplicator, Deduplicator, FileActionInfo};
use crate::log_replay::{
    ActionsBatch, FileActionDeduplicator, FileActionKey, LogReplayProcessor,
    ParallelLogReplayProcessor,
};
use crate::log_segment::CheckpointReadInfo;
use crate::scan::transform_spec::{get_transform_expr, parse_partition_values, TransformSpec};
use crate::scan::Scalar;
use crate::schema::{
    schema_ref, ColumnMetadataKey, ColumnNamesAndTypes, DataType, MapType, PrimitiveType,
    SchemaRef, SchemaStructPatchBuilder, StructField, StructType, ToSchema as _,
};
use crate::table_features::ColumnMappingMode;
use crate::utils::{require, FoldWithOption as _};
use crate::{DeltaResult, Engine, Error, ExpressionEvaluator};

/// Read-time stats toggles consumed by [`ScanLogReplayProcessor`].
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct ScanStatsOptions {
    /// Skip reading file statistics entirely. Disables data skipping.
    pub(crate) skip_stats: bool,
    /// Synthesize the `stats` JSON output via `ToJson(stats_parsed)` on checkpoints
    /// whose `add.stats` is null but whose `add.stats_parsed` is populated
    /// (writeStatsAsJson=false, writeStatsAsStruct=true). When false, `ScanFile.stats`
    /// is left null on such checkpoints; engines that consume `stats_parsed` directly
    /// avoid the per-batch `ToJson` cost.
    pub(crate) synthesize_json: bool,
}

impl Default for ScanStatsOptions {
    fn default() -> Self {
        Self {
            skip_stats: false,
            synthesize_json: true,
        }
    }
}

/// Read-time partition value toggles consumed by [`ScanLogReplayProcessor`].
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct ScanPartitionValuesOptions {
    /// Emit the typed `partitionValues_parsed` struct column in scan metadata output,
    /// independent of any predicate.
    pub(crate) parsed_struct: bool,
    /// Session timezone for resolving offset-less `TIMESTAMP` partition value strings
    /// reconstructed from the string map; `None` = UTC. See
    /// [`PartitionValuesOptions::with_session_timezone`].
    ///
    /// [`PartitionValuesOptions::with_session_timezone`]: crate::scan::PartitionValuesOptions::with_session_timezone
    pub(crate) session_timezone: Option<String>,
}

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
    #[serde(default)]
    stats_options: ScanStatsOptions,
    #[serde(default)]
    partition_values_options: ScanPartitionValuesOptions,
    /// Physical partition schema for checkpoint partition pruning via `partitionValues_parsed`
    physical_partition_schema: Option<SchemaRef>,
    /// Physical leaf paths which are expected to have stats collected. Carried alongside
    /// `physical_stats_schema` so the distributed `DataSkippingFilter` rebuilds the same
    /// filter the sequential phase used. `#[serde(default)]` keeps older blobs readable:
    /// an empty set drops every data-column reference, which means no skipping but is
    /// still correct.
    #[serde(default)]
    physical_stats_columns: HashSet<ColumnName>,
    #[serde(default)]
    is_catalog_managed: bool,
    skip_row_transforms: bool,
}

/// Serializable processor state for distributed processing. This can be serialized using the
/// default serde serialization, or through custom serialization in the engine.
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

/// [`ScanLogReplayProcessor`] performs log replay (processes actions) specifically for doing a
/// table scan.
///
/// During a table scan, the processor reads batches of log actions (in reverse chronological order)
/// and performs the following steps:
///
/// - Data Skipping: Applies a predicate-based filter (via [`DataSkippingFilter`]) to quickly skip
///   files that are irrelevant for the query. This includes both data column stats
///   (min/max/nullCount) and partition value filtering in a single columnar pass. A secondary
///   row-level partition filter catches remaining files the columnar pass cannot prune (e.g. null
///   partition values where null-safety conservatively keeps them).
/// - Action Deduplication: Leverages the [`FileActionDeduplicator`] to ensure that for each unique
///   file (identified by its path and deletion vector unique ID), only the latest valid Add action
///   is processed.
/// - Transformation: Applies a built-in transformation (`log_transform` or `checkpoint_transform`)
///   to convert selected Add actions into [`ScanMetadata`], the intermediate format passed to the
///   engine.
/// - Row StructPatch passthrough: Any user-provided row-level transformation expressions (e.g.
///   those derived from projection or filters) are preserved and passed through to the engine,
///   which applies them as part of its scan execution logic.
///
/// As an implementation of [`LogReplayProcessor`], [`ScanLogReplayProcessor`] provides the
/// `process_actions_batch` method, which applies these steps to each batch of log actions and
/// produces a [`ScanMetadata`] result. This result includes the transformed batch, a selection
/// vector indicating which rows are valid, and any row-level transformation expressions that need
/// to be applied to the selected rows.
#[allow(rustdoc::broken_intra_doc_links, rustdoc::private_intra_doc_links)]
pub struct ScanLogReplayProcessor {
    data_skipping_filter: Option<DataSkippingFilter>,
    /// StructPatch for log batches (commit files) - uses ParseJson for stats and MapToStruct
    /// for partition values
    log_transform: Arc<dyn ExpressionEvaluator>,
    /// StructPatch for checkpoint batches - reads pre-parsed stats_parsed and
    /// partitionValues_parsed directly when available, otherwise parses from raw columns
    checkpoint_transform: Arc<dyn ExpressionEvaluator>,
    state_info: Arc<StateInfo>,
    /// A set of (data file path, dv_unique_id) pairs that have been seen thus
    /// far in the log. This is used to filter out files with Remove actions as
    /// well as duplicate entries in the log.
    seen_file_keys: HashSet<FileActionKey>,
    /// Read-time stats options.
    stats_options: ScanStatsOptions,
    /// Read-time partition value options.
    partition_values_options: ScanPartitionValuesOptions,
    /// Information about checkpoint reading for stats optimization
    checkpoint_info: CheckpointReadInfo,
    /// Metrics related to the scan
    metrics: Arc<ScanMetrics>,
}

impl ScanLogReplayProcessor {
    // These index positions correspond to the order of columns defined in
    // `selected_column_names_and_types()`
    const ADD_PATH_INDEX: usize = 0; // Position of "add.path" in getters
    const ADD_PARTITION_VALUES_INDEX: usize = 1; // Position of "add.partitionValues" in getters
    const ADD_SIZE_INDEX: usize = 2; // Position of "add.size" in getters
    const ADD_DV_START_INDEX: usize = 3; // Start position of add deletion vector columns
    const BASE_ROW_ID_INDEX: usize = 6; // Position of add.baseRowId in getters
    const REMOVE_PATH_INDEX: usize = 7; // Position of "remove.path" in getters
    const REMOVE_DV_START_INDEX: usize = 8; // Start position of remove deletion vector columns

    /// Create a new [`ScanLogReplayProcessor`] instance
    pub(crate) fn new(
        engine: &dyn Engine,
        state_info: Arc<StateInfo>,
        checkpoint_info: CheckpointReadInfo,
        stats_options: ScanStatsOptions,
        partition_values_options: ScanPartitionValuesOptions,
    ) -> DeltaResult<Self> {
        let dedup_capacity = state_info.dedup_capacity_hint();
        Self::new_with_seen_files(
            engine,
            state_info,
            checkpoint_info,
            HashSet::with_capacity(dedup_capacity),
            stats_options,
            partition_values_options,
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
    /// - `stats_options`: Read-time stats options (see [`ScanStatsOptions`])
    /// - `partition_values_options`: Read-time partition value options (see
    ///   [`ScanPartitionValuesOptions`])
    pub(crate) fn new_with_seen_files(
        engine: &dyn Engine,
        state_info: Arc<StateInfo>,
        checkpoint_info: CheckpointReadInfo,
        seen_file_keys: HashSet<FileActionKey>,
        stats_options: ScanStatsOptions,
        partition_values_options: ScanPartitionValuesOptions,
    ) -> DeltaResult<Self> {
        let CheckpointReadInfo {
            has_stats_parsed,
            has_partition_values_parsed,
            checkpoint_read_schema,
        } = checkpoint_info.clone();
        let ScanStatsOptions {
            skip_stats,
            synthesize_json,
        } = stats_options;

        // Create metrics first so we can pass them to DataSkippingFilter
        let metrics = Arc::new(ScanMetrics::default());

        // Extract the physical predicate for data skipping and partition filtering.
        // DataSkippingFilter expects Option<(PredicateRef, SchemaRef)>.
        let physical_predicate = match &state_info.physical_predicate {
            PhysicalPredicate::Some(predicate, schema) => Some((predicate.clone(), schema.clone())),
            _ => None,
        };

        // When skip_stats is enabled, disable both data column skipping and partition pruning.
        // Both rely on the same DataSkippingFilter columnar pass, so they are controlled together.
        let stats_schema_for_transform = if skip_stats {
            None
        } else {
            state_info.physical_stats_schema.clone()
        };

        // The partition schema feeds two consumers: the DataSkippingFilter (predicate
        // pruning, disabled by skip_stats together with stats) and the engine-facing
        // `partitionValues_parsed` output column (requested via `parsed_struct`, independent
        // of skip_stats). Either consumer keeps the transform emitting the column.
        let partition_schema_for_transform =
            if partition_values_options.parsed_struct || !skip_stats {
                state_info.physical_partition_schema.clone()
            } else {
                None
            };

        let output_schema = scan_row_schema_with_parsed_columns(
            stats_schema_for_transform.clone(),
            partition_schema_for_transform.clone(),
            partition_values_options.session_timezone.as_deref(),
        )?;

        // A native checkpoint `partitionValues_parsed` column bakes offset-less TIMESTAMP values at
        // the writer's zone (kernel resolves them as UTC on write), which a session-aware reader
        // cannot recover from the frozen instant. So when the engine supplies a session zone AND
        // the table has a TIMESTAMP partition column, bypass the frozen column and
        // reconstruct partition values from the raw `partitionValues` string map, resolving
        // them in the session zone. Every other partition type (date, int, string,
        // `TIMESTAMP_NTZ`) resolves identically regardless of zone, so those tables keep
        // the native fast path even under a session zone.
        let reparse_partition_values_from_map = partition_values_options.session_timezone.is_some()
            && partition_schema_has_zoned_timestamp(partition_schema_for_transform.as_ref());
        let checkpoint_has_partition_values_parsed =
            has_partition_values_parsed && !reparse_partition_values_from_map;

        // Create data skipping filter that reads stats_parsed and partitionValues_parsed
        // from the transformed batch. This avoids double JSON parsing -- the transform parses
        // JSON once, then data skipping reads the already-parsed columns from the output.
        //
        // When partition columns are referenced by the predicate, the filter receives a
        // partition schema + expression to extract typed partition values. Since the transform
        // already produces `partitionValues_parsed`, the filter reads that column directly.
        let data_skipping_filter = if skip_stats {
            None
        } else {
            DataSkippingFilter::new(
                engine,
                physical_predicate.as_ref().map(|(p, _)| p.clone()),
                stats_schema_for_transform.as_ref(),
                column_expr_ref!("stats_parsed"),
                partition_schema_for_transform.as_ref(),
                column_expr_ref!("partitionValues_parsed"),
                // The transform flattens `add.*` to top-level columns, so `path` is non-null
                // exactly for Add rows.
                Arc::new(Predicate::is_not_null(column_expr!("path")).into()),
                output_schema.clone(),
                &state_info.physical_stats_columns,
                Some(metrics.clone()),
            )
        };

        Ok(Self {
            data_skipping_filter,
            // Log transform: parse JSON for stats, MapToStruct for partition values
            log_transform: engine.evaluation_handler().new_expression_evaluator(
                checkpoint_read_schema.clone(),
                get_add_transform_expr(
                    stats_schema_for_transform.clone(),
                    false,
                    skip_stats,
                    synthesize_json,
                    partition_schema_for_transform.clone(),
                    false,
                ),
                output_schema.clone().into(),
            )?,
            // Checkpoint transform: read pre-parsed columns directly when available
            checkpoint_transform: engine.evaluation_handler().new_expression_evaluator(
                checkpoint_read_schema,
                get_add_transform_expr(
                    stats_schema_for_transform,
                    has_stats_parsed,
                    skip_stats,
                    synthesize_json,
                    partition_schema_for_transform,
                    checkpoint_has_partition_values_parsed,
                ),
                output_schema.into(),
            )?,
            seen_file_keys,
            state_info,
            stats_options,
            partition_values_options,
            checkpoint_info,
            metrics,
        })
    }

    /// Get a reference to the checkpoint info.
    pub(crate) fn checkpoint_info(&self) -> &CheckpointReadInfo {
        &self.checkpoint_info
    }

    pub(crate) fn get_metrics(&self) -> &ScanMetrics {
        self.metrics.as_ref()
    }

    pub(crate) fn is_catalog_managed(&self) -> bool {
        self.state_info.is_catalog_managed
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
            physical_partition_schema,
            physical_stats_columns,
            is_catalog_managed,
            skip_row_transforms,
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
            stats_options: self.stats_options,
            partition_values_options: self.partition_values_options,
            physical_partition_schema,
            physical_stats_columns,
            is_catalog_managed,
            skip_row_transforms,
        };
        let internal_state_blob = serde_json::to_vec(&internal_state)
            .map_err(|e| Error::generic(format!("Failed to serialize internal state: {e}")))?;

        Ok(SerializableScanState {
            predicate,
            internal_state_blob,
            seen_file_keys: self.seen_file_keys,
            checkpoint_info: self.checkpoint_info,
        })
    }

    /// Reconstruct a processor from serialized state.
    ///
    /// Creates a new processor with the provided state. All fields (partition_filter,
    /// data_skipping_filter, log_transform, checkpoint_transform, and seen_file_keys) are
    /// reconstructed from the serialized state and engine.
    ///
    /// # Parameters
    /// - `engine`: Engine for creating evaluators and filters
    /// - `state`: The serialized state containing predicate, internal state blob, and seen file
    ///   keys
    ///
    /// # Returns
    /// A new `ScanLogReplayProcessor` wrapped in an Arc.
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
            physical_partition_schema: internal_state.physical_partition_schema,
            physical_stats_columns: internal_state.physical_stats_columns,
            is_catalog_managed: internal_state.is_catalog_managed,
            skip_row_transforms: internal_state.skip_row_transforms,
        });

        Self::new_with_seen_files(
            engine,
            state_info,
            state.checkpoint_info,
            state.seen_file_keys,
            internal_state.stats_options,
            internal_state.partition_values_options,
        )
    }
}

/// A visitor that deduplicates a stream of add and remove actions into a stream of valid adds. Log
/// replay visits actions newest-first, so once we've seen a file action for a given (path, dvId)
/// pair, we should ignore all subsequent (older) actions for that same (path, dvId) pair. If the
/// first action for a given file is a remove, then that file does not show up in the result at all.
struct AddRemoveDedupVisitor<'a, D: Deduplicator> {
    deduplicator: D,
    selection_vector: Vec<bool>,
    state_info: Arc<StateInfo>,
    row_transform_exprs: Vec<Option<ExpressionRef>>,
    metrics: &'a ScanMetrics,
}

impl<'a, D: Deduplicator> AddRemoveDedupVisitor<'a, D> {
    fn new(
        deduplicator: D,
        selection_vector: Vec<bool>,
        state_info: Arc<StateInfo>,
        metrics: &'a ScanMetrics,
    ) -> AddRemoveDedupVisitor<'a, D> {
        AddRemoveDedupVisitor {
            deduplicator,
            selection_vector,
            state_info,
            row_transform_exprs: Vec::new(),
            metrics,
        }
    }

    /// True if this row contains an Add action that should survive log replay. Skip it if the row
    /// is not an Add action, or the file has already been seen previously.
    fn is_valid_add<'b>(
        &mut self,
        row: usize,
        getters: &[&'b dyn GetData<'b>],
    ) -> DeltaResult<bool> {
        // When processing file actions, we extract path and deletion vector information based on
        // action type:
        // - For Add actions: path is at index 0, size at 2, then followed by DV fields at indexes
        //   3-5
        // - For Remove actions (in log batches only): path is at index 7, followed by DV fields at
        //   indexes 8-10
        // The file extraction logic selects the appropriate indexes based on whether we found a
        // valid path. Remove getters are not included when visiting a non-log batch
        // (checkpoint batch), so do not try to extract remove actions in that case.
        let Some(FileActionInfo {
            key: file_key,
            size,
            is_add,
        }) = self.deduplicator.extract_file_action(
            row,
            getters,
            !self.deduplicator.is_log_batch(), // skip_removes. true if this is a checkpoint batch
        )?
        else {
            self.metrics.incr_non_file_actions();
            return Ok(false);
        };

        if is_add {
            self.metrics.incr_add_files_seen()
        } else {
            self.metrics.incr_remove_files_seen()
        };

        // Parse partition values for building the per-row transform expression.
        // Partition pruning is handled by DataSkippingFilter in the columnar data skipping phase,
        // so we only need to parse values here for the transform.
        let partition_values = match &self.state_info.transform_spec {
            Some(transform) if is_add && !self.state_info.skip_row_transforms => {
                let partition_values = getters[ScanLogReplayProcessor::ADD_PARTITION_VALUES_INDEX]
                    .get(row, "add.partitionValues")?;
                parse_partition_values(
                    &self.state_info.logical_schema,
                    transform,
                    &partition_values,
                    self.state_info.column_mapping_mode,
                )?
            }
            _ => Default::default(),
        };

        // Check both adds and removes (skipping already-seen), but only transform and return adds
        if self.deduplicator.check_and_record_seen(file_key) || !is_add {
            return Ok(false);
        }
        if !self.state_info.skip_row_transforms {
            let base_row_id: Option<i64> =
                getters[ScanLogReplayProcessor::BASE_ROW_ID_INDEX].get_opt(row, "add.baseRowId")?;
            let patch_expr = self
                .state_info
                .transform_spec
                .as_ref()
                .map(|transform_spec| {
                    get_transform_expr(
                        transform_spec,
                        partition_values,
                        &self.state_info.physical_schema,
                        base_row_id,
                    )
                })
                .transpose()?;
            if patch_expr.is_some() {
                // fill in any needed `None`s for previous rows
                self.row_transform_exprs.resize_with(row, Default::default);
                self.row_transform_exprs.push(patch_expr);
            }
        }
        self.metrics.record_active_add_file(size);
        Ok(true)
    }
}

impl<D: Deduplicator> RowVisitor for AddRemoveDedupVisitor<'_, D> {
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
                (LONG, column_name!("add.size")),
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
            let add_count = ScanLogReplayProcessor::REMOVE_PATH_INDEX;
            (&names[..add_count], &types[..add_count])
        }
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        let start = std::time::Instant::now();

        let is_log_batch = self.deduplicator.is_log_batch();
        let expected_getters = if is_log_batch { 11 } else { 7 };
        require!(
            getters.len() == expected_getters,
            Error::InternalError(format!(
                "Wrong number of AddRemoveDedupVisitor getters: {}",
                getters.len()
            ))
        );

        for row in 0..row_count {
            if self.selection_vector[row] {
                self.selection_vector[row] = self.is_valid_add(row, getters)?;
            }
        }

        self.metrics
            .add_dedup_visitor_time_ns(start.elapsed().as_nanos() as u64);

        Ok(())
    }
}

pub(crate) static FILE_CONSTANT_VALUES_NAME: &str = "fileConstantValues";
pub(crate) static BASE_ROW_ID_NAME: &str = "baseRowId";
pub(crate) static DEFAULT_ROW_COMMIT_VERSION_NAME: &str = "defaultRowCommitVersion";
pub(crate) static CLUSTERING_PROVIDER_NAME: &str = "clusteringProvider";
pub(crate) static TAGS_NAME: &str = "tags";
pub(crate) static STATS_PARSED_NAME: &str = "stats_parsed";
#[internal_api]
pub(crate) static PARTITION_VALUES_PARSED_NAME: &str = "partitionValues_parsed";

// NB: If you update this schema, ensure you update the comment describing it in the doc comment
// for `scan_row_schema` in scan/mod.rs! You'll also need to update ScanFileVisitor as the
// indexes will be off, and [`get_add_transform_expr`] below to match it.
pub(crate) static SCAN_ROW_SCHEMA: LazyLock<Arc<StructType>> = LazyLock::new(|| {
    // Note that fields projected out of a nullable struct must be nullable
    schema_ref! {
        nullable "path": STRING,
        nullable "size": LONG,
        nullable "modificationTime": LONG,
        nullable "stats": STRING,
        nullable "deletionVector": (DeletionVectorDescriptor::to_schema()),
        nullable FILE_CONSTANT_VALUES_NAME: {
            nullable "partitionValues": { STRING => nullable STRING },
            nullable BASE_ROW_ID_NAME: LONG,
            nullable DEFAULT_ROW_COMMIT_VERSION_NAME: LONG,
            nullable "tags": { STRING => nullable STRING },
            nullable CLUSTERING_PROVIDER_NAME: STRING,
        },
    }
});

/// Build the scan-row schema, appending the opt-in typed `stats_parsed` / `partitionValues_parsed`
/// columns when requested.
///
/// These typed columns are appended at the top level (siblings of `fileConstantValues`) rather than
/// nested inside it. This mirrors `stats_parsed`, keeps `fileConstantValues` a fixed shape
/// regardless of the engine's options, and keeps data-skipping paths uniform:
/// `partitionValues_parsed.<col>` parallels `stats_parsed.minValues.<col>`. The checkpoint source
/// is also `add.partitionValues_parsed`, a sibling of `add.stats_parsed`.
///
/// `session_timezone`, when set, is stamped onto the `TIMESTAMP` leaves of the
/// `partitionValues_parsed` sub-struct via [`ColumnMetadataKey::SessionTimezone`]. That schema is
/// the `MapToStruct` transform's output type, so the arrow evaluator reads the zone back from it to
/// resolve offset-less `TIMESTAMP` partition strings, keeping the expression itself generic. The
/// key never reaches the engine: it is stripped when the schema crosses into Arrow.
fn scan_row_schema_with_parsed_columns(
    stats_schema: Option<SchemaRef>,
    partition_schema: Option<SchemaRef>,
    session_timezone: Option<&str>,
) -> DeltaResult<SchemaRef> {
    let needs_extra = stats_schema.is_some() || partition_schema.is_some();
    if !needs_extra {
        return Ok(SCAN_ROW_SCHEMA.clone());
    }
    let partition_schema =
        partition_schema.map(|schema| annotate_partition_timezone(&schema, session_timezone));
    let patch = SchemaStructPatchBuilder::new()
        .fold_with(stats_schema.as_ref(), |patch, schema| {
            patch.append(StructField::nullable(STATS_PARSED_NAME, schema.clone()))
        })
        .fold_with(partition_schema.as_ref(), |patch, schema| {
            patch.append(StructField::nullable(
                PARTITION_VALUES_PARSED_NAME,
                schema.clone(),
            ))
        });
    Ok(Arc::new(patch.build(&SCAN_ROW_SCHEMA)?))
}

/// Whether `field` is a partition column whose resolved instant depends on the session zone. Only
/// zoned `TIMESTAMP` qualifies; `TIMESTAMP_NTZ` is a zoneless wall-clock and every other type is
/// zone-independent. Single source of truth shared by the checkpoint-bypass gate
/// ([`partition_schema_has_zoned_timestamp`]) and the annotation stamper
/// ([`annotate_partition_timezone`]), which must agree on exactly which leaves the zone applies to.
fn partition_field_is_zoned_timestamp(field: &StructField) -> bool {
    field.data_type() == &DataType::Primitive(PrimitiveType::Timestamp)
}

/// Returns whether `partition_schema` has a zoned `TIMESTAMP` column. Partition columns are flat
/// primitives, so only top-level leaves are inspected.
fn partition_schema_has_zoned_timestamp(partition_schema: Option<&SchemaRef>) -> bool {
    partition_schema.is_some_and(|schema| schema.fields().any(partition_field_is_zoned_timestamp))
}

/// Stamps `session_timezone` onto each zoned `TIMESTAMP` field of `partition_schema` via
/// [`ColumnMetadataKey::SessionTimezone`], returning the schema unchanged when the zone is `None`.
/// Partition columns are flat primitives, so only top-level leaves need annotating.
fn annotate_partition_timezone(
    partition_schema: &StructType,
    session_timezone: Option<&str>,
) -> SchemaRef {
    let Some(tz) = session_timezone else {
        return Arc::new(partition_schema.clone());
    };
    let fields = partition_schema.fields().map(|field| {
        if partition_field_is_zoned_timestamp(field) {
            field.clone().add_metadata([(
                ColumnMetadataKey::SessionTimezone.as_ref().to_string(),
                tz.to_string(),
            )])
        } else {
            field.clone()
        }
    });
    Arc::new(StructType::new_unchecked(fields))
}

/// Build the add transform expression with optional stats and partition value parsing.
///
/// # Parameters
/// - `physical_stats_schema`: Schema for parsing stats from JSON and for output (physical column
///   names), or None if stats should not be included in output.
/// - `has_stats_parsed`: Whether checkpoint has pre-parsed stats_parsed column. When true and
///   `synthesize_json` is true, stats output uses `COALESCE(add.stats, ToJson(add.stats_parsed))`
///   so that `ScanFile.stats` is populated even when the checkpoint lacks JSON stats
///   (writeStatsAsJson=false).
/// - `skip_stats`: When true, replaces the stats column with a null literal, avoiding reads of the
///   raw stats JSON string from checkpoint parquet files.
/// - `synthesize_json`: When false, disables the `ToJson(add.stats_parsed)` fallback regardless of
///   `has_stats_parsed`. Set false for engines that consume `stats_parsed` directly and don't want
///   to pay the per-batch `ToJson` cost over potentially large stats structs.
/// - `partition_schema`: Schema of typed partition columns for data skipping, or None if partition
///   value parsing is not needed.
/// - `has_partition_values_parsed`: Whether the source carries a native `partitionValues_parsed`
///   column (checkpoint). When true it is read directly; otherwise the struct is reconstructed from
///   the `partitionValues` string map.
///
/// The transform includes `stats_parsed` only when `physical_stats_schema` is Some,
/// and `partitionValues_parsed` only when `partition_schema` is Some. When reconstructing from the
/// string map, the session zone for resolving offset-less `TIMESTAMP` values rides on
/// `partition_schema`'s field metadata (see [`scan_row_schema_with_parsed_columns`]), so the
/// `MapToStruct` expression stays generic.
/// Stats are output using physical column names.
fn get_add_transform_expr(
    physical_stats_schema: Option<SchemaRef>,
    has_stats_parsed: bool,
    skip_stats: bool,
    synthesize_json: bool,
    partition_schema: Option<SchemaRef>,
    has_partition_values_parsed: bool,
) -> ExpressionRef {
    let stats_expr = if skip_stats {
        Arc::new(Expression::Literal(Scalar::Null(DataType::STRING)))
    } else if has_stats_parsed && synthesize_json {
        // Checkpoint may lack JSON stats when writeStatsAsJson=false. Fall back to
        // serializing stats_parsed so ScanFile.stats is populated either way.
        Arc::new(Expression::coalesce([
            Expression::column(["add", "stats"]),
            Expression::unary(
                UnaryExpressionOp::ToJson,
                Expression::column(["add", "stats_parsed"]),
            ),
        ]))
    } else {
        column_expr_ref!("add.stats")
    };
    let mut fields = vec![
        column_expr_ref!("add.path"),
        column_expr_ref!("add.size"),
        column_expr_ref!("add.modificationTime"),
        stats_expr,
        column_expr_ref!("add.deletionVector"),
        Arc::new(Expression::struct_from([
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

    // Add partitionValues_parsed when partition columns are needed for data skipping or for the
    // engine-facing typed output column.
    if partition_schema.is_some() {
        let pv_parsed_expr = if has_partition_values_parsed {
            // Checkpoint carries a native partitionValues_parsed column - read it directly.
            column_expr!("add.partitionValues_parsed")
        } else {
            // No native column (JSON commit): reconstruct from the string map. The output schema
            // carries the session zone for resolving offset-less TIMESTAMP values.
            Expression::map_to_struct(column_expr!("add.partitionValues"))
        };
        fields.push(Arc::new(pv_parsed_expr));
    }

    Arc::new(Expression::struct_from(fields))
}

// TODO: Move this to transaction/mod.rs once `scan_metadata_from` is pub, as this is used for
// deletion vector update transformations.
#[allow(unused)]
pub(crate) fn get_scan_metadata_transform_expr() -> ExpressionRef {
    static EXPR: LazyLock<ExpressionRef> = LazyLock::new(|| {
        Arc::new(Expression::struct_from([Arc::new(
            Expression::struct_from([
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
            ]),
        )]))
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

        // Step 2: Build selection vector from TRANSFORMED batch (reads stats_parsed directly).
        // This avoids double JSON parsing -- the transform already parsed the stats.
        // Data skipping is safe for Remove rows: their add-side columns (stats_parsed,
        // partitionValues_parsed) are null. For stats, the skipping predicate wraps comparisons
        // with ISNULL guards that keep rows with missing stats. For partition values, the
        // predicate is wrapped with OR(NOT is_add, pred) via guard_for_removes, so non-Add
        // rows always pass the partition filter regardless of null partition values.
        let selection_vector = self.build_selection_vector(transformed.as_ref())?;
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
            Self::ADD_SIZE_INDEX,
            Self::ADD_DV_START_INDEX,
        )?;
        let mut visitor = AddRemoveDedupVisitor::new(
            deduplicator,
            selection_vector,
            self.state_info.clone(),
            &self.metrics,
        );
        visitor.visit_rows_of(actions.as_ref())?;

        // Step 4: Return transformed batch with updated selection vector
        let scan_metadata = ScanMetadata::try_new(
            transformed,
            visitor.selection_vector,
            visitor.row_transform_exprs,
        )?;
        self.metrics
            .update_peak_hash_set_size(self.seen_file_keys.len());
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

        // Step 1: Apply transform FIRST (outputs stats_parsed and partitionValues_parsed).
        // Use the correct transform based on batch type:
        // - Log batches: parse JSON for stats, MapToStruct for partition values
        // - Checkpoint batches: read pre-parsed columns directly when available
        let transform = if is_log_batch {
            &self.log_transform
        } else {
            &self.checkpoint_transform
        };
        let transformed = transform.evaluate(actions.as_ref())?;
        require!(
            transformed.len() == actions.len(),
            Error::internal_error(format!(
                "transform output length {} != actions length {}",
                transformed.len(),
                actions.len()
            ))
        );

        // Step 2: Build selection vector from TRANSFORMED batch (reads stats_parsed directly).
        // This avoids double JSON parsing -- the transform already parsed the stats.
        // Data skipping is safe for Remove rows: their add-side columns (stats_parsed,
        // partitionValues_parsed) are null. For stats, the skipping predicate wraps comparisons
        // with ISNULL guards that keep rows with missing stats. For partition values, the
        // predicate is wrapped with OR(NOT is_add, pred) via guard_for_removes, so non-Add
        // rows always pass the partition filter regardless of null partition values.
        let selection_vector = self.build_selection_vector(transformed.as_ref())?;
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
            Self::ADD_SIZE_INDEX,
            Self::REMOVE_PATH_INDEX,
            Self::ADD_DV_START_INDEX,
            Self::REMOVE_DV_START_INDEX,
        );
        let mut visitor = AddRemoveDedupVisitor::new(
            deduplicator,
            selection_vector,
            self.state_info.clone(),
            &self.metrics,
        );
        visitor.visit_rows_of(actions.as_ref())?;

        // Step 4: Return transformed batch with updated selection vector
        let scan_metadata = ScanMetadata::try_new(
            transformed,
            visitor.selection_vector,
            visitor.row_transform_exprs,
        )?;
        self.metrics
            .update_peak_hash_set_size(self.seen_file_keys.len());
        Ok(scan_metadata)
    }

    fn data_skipping_filter(&self) -> Option<&DataSkippingFilter> {
        self.data_skipping_filter.as_ref()
    }
}

/// Given an iterator of [`ActionsBatch`]s (batches of actions read from the log) and a predicate,
/// returns a tuple of:
/// 1. An iterator of [`ScanMetadata`]s (which includes the files to be scanned as
///    [`FilteredEngineData`] and transforms that must be applied to correctly read the data).
/// 2. An `Arc<ScanMetrics>` containing metrics collected during log replay.
///
/// Each row that is selected in the returned `engine_data` _must_ be processed to complete the
/// scan. Non-selected rows _must_ be ignored.
///
/// When `stats_options.skip_stats` is true, file statistics are not read from checkpoint parquet
/// files and columnar data skipping is disabled (no stats-based or partition-value-based
/// pruning), but row-level partition filtering still applies.
///
/// Note: The iterator of [`ActionsBatch`]s ('action_iter' parameter) must be sorted by the order of
/// the actions in the log from most recent to least recent.
pub(crate) fn scan_action_iter(
    engine: &dyn Engine,
    action_iter: impl Iterator<Item = DeltaResult<ActionsBatch>>,
    state_info: Arc<StateInfo>,
    checkpoint_info: CheckpointReadInfo,
    stats_options: ScanStatsOptions,
    partition_values_options: ScanPartitionValuesOptions,
) -> DeltaResult<(
    impl Iterator<Item = DeltaResult<ScanMetadata>>,
    Arc<ScanMetrics>,
)> {
    let processor = ScanLogReplayProcessor::new(
        engine,
        state_info,
        checkpoint_info,
        stats_options,
        partition_values_options,
    )?;
    let metrics = processor.metrics.clone();
    Ok((processor.process_actions_iter(action_iter), metrics))
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use rstest::rstest;

    use super::{
        annotate_partition_timezone, get_add_transform_expr, partition_schema_has_zoned_timestamp,
        scan_action_iter, InternalScanState, ScanLogReplayProcessor, ScanPartitionValuesOptions,
        ScanStatsOptions, SerializableScanState,
    };
    use crate::actions::get_commit_schema;
    use crate::engine::sync::SyncEngine;
    use crate::expressions::{
        BinaryExpressionOp, Expression, OpaquePredicateOp, Predicate, Scalar,
        ScalarExpressionEvaluator, UnaryExpressionOp,
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
        add_batch_with_remove, add_batch_with_remove_and_partition, run_with_validate_callback,
    };
    use crate::scan::PhysicalPredicate;
    use crate::schema::{
        schema_ref, DataType, MetadataColumnSpec, SchemaRef, StructField, StructType,
    };
    use crate::table_features::ColumnMappingMode;
    use crate::utils::test_utils::assert_result_error_with_message;
    use crate::{DeltaResult, Expression as Expr, ExpressionRef};

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
            physical_partition_schema: None,
            physical_stats_columns: HashSet::new(),
            is_catalog_managed: false,
            skip_row_transforms: false,
        });
        let (iter, _metrics) = scan_action_iter(
            &SyncEngine::new(),
            batch
                .into_iter()
                .map(|batch| Ok(ActionsBatch::new(batch as _, true))),
            state_info,
            test_checkpoint_info(),
            ScanStatsOptions::default(),
            ScanPartitionValuesOptions::default(),
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
        let (iter, _metrics) = scan_action_iter(
            &SyncEngine::new(),
            batch
                .into_iter()
                .map(|batch| Ok(ActionsBatch::new(batch as _, true))),
            Arc::new(state_info),
            test_checkpoint_info(),
            ScanStatsOptions::default(),
            ScanPartitionValuesOptions::default(),
        )
        .unwrap();

        fn validate_patch(patch_expr: Option<&ExpressionRef>, expected_date_offset: i32) {
            assert!(patch_expr.is_some());
            let Expr::StructPatch(patch) = patch_expr.unwrap().as_ref() else {
                panic!("Expression should always be a StructPatch expr");
            };

            // With sparse patches, we expect only one insertion for the partition column.
            assert!(patch.prepended_fields.is_empty());
            assert!(patch.appended_fields.is_empty());
            let mut field_patches = patch.field_patches.iter();
            let (field_name, field_patch) = field_patches.next().unwrap();
            assert_eq!(field_name, "value");
            assert!(field_patch.keep_input);
            let [expr] = &field_patch.insertions[..] else {
                panic!("Expected a single insertion");
            };
            let Expr::Literal(Scalar::Date(date_offset)) = expr.as_ref() else {
                panic!("Expected a literal date");
            };
            assert_eq!(*date_offset, expected_date_offset);
            assert!(field_patches.next().is_none());
        }

        for res in iter {
            let scan_metadata = res.unwrap();
            let transforms = scan_metadata.scan_file_transforms;
            // in this case we have a metadata action first and protocol 3rd, so we expect 4 items,
            // the first and 3rd being a `None`
            assert_eq!(transforms.len(), 4, "Should have 4 transforms");
            assert!(transforms[0].is_none(), "transform at [0] should be None");
            assert!(transforms[2].is_none(), "transform at [2] should be None");
            validate_patch(transforms[1].as_ref(), 17511);
            validate_patch(transforms[3].as_ref(), 17510);
        }
    }

    #[test]
    fn test_row_id_patch() {
        let schema: SchemaRef = schema_ref! { nullable "value": INTEGER };
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
        let (iter, _metrics) = scan_action_iter(
            &SyncEngine::new(),
            batch
                .into_iter()
                .map(|batch| Ok(ActionsBatch::new(batch as _, true))),
            Arc::new(state_info),
            test_checkpoint_info(),
            ScanStatsOptions::default(),
            ScanPartitionValuesOptions::default(),
        )
        .unwrap();

        for res in iter {
            let scan_metadata = res.unwrap();
            let transforms = scan_metadata.scan_file_transforms;
            assert_eq!(transforms.len(), 1, "Should have 1 transform");
            if let Some(Expr::StructPatch(patch)) = transforms[0].as_ref().map(Arc::as_ref) {
                assert!(patch.input_path.is_none());
                let row_id_patch = patch
                    .field_patches
                    .get("row_id_col")
                    .expect("Should have row_id_col patch");
                assert!(!row_id_patch.keep_input);
                assert_eq!(row_id_patch.insertions.len(), 1);
                let expr = &row_id_patch.insertions[0];
                let expected_expr = Arc::new(Expr::coalesce([
                    Expr::column(["row_id_col"]),
                    Expr::binary(
                        BinaryExpressionOp::Plus,
                        Expr::literal(42i64),
                        Expr::column(["row_indexes_for_row_id_0"]),
                    ),
                ]));
                assert_eq!(expr, &expected_expr);
            } else {
                panic!("Should have been a StructPatch expression");
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
            ScanStatsOptions::default(),
            ScanPartitionValuesOptions::default(),
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
            ScanStatsOptions::default(),
            ScanPartitionValuesOptions::default(),
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
            ScanStatsOptions::default(),
            ScanPartitionValuesOptions::default(),
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
            let schema: SchemaRef = schema_ref! { nullable "id": INTEGER };
            let state_info = Arc::new(StateInfo {
                logical_schema: schema.clone(),
                physical_schema: schema,
                physical_predicate: PhysicalPredicate::None,
                transform_spec: None,
                column_mapping_mode: mode,
                physical_stats_schema: None,
                physical_partition_schema: None,
                physical_stats_columns: HashSet::new(),
                is_catalog_managed: false,
                skip_row_transforms: false,
            });
            let checkpoint_info = test_checkpoint_info();
            let processor = ScanLogReplayProcessor::new(
                &engine,
                state_info,
                checkpoint_info.clone(),
                ScanStatsOptions::default(),
                ScanPartitionValuesOptions::default(),
            )
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
        let schema: SchemaRef = schema_ref! { nullable "id": INTEGER };
        let state_info = Arc::new(StateInfo {
            logical_schema: schema.clone(),
            physical_schema: schema,
            physical_predicate: PhysicalPredicate::None,
            transform_spec: None,
            column_mapping_mode: ColumnMappingMode::None,
            physical_stats_schema: None,
            physical_partition_schema: None,
            physical_stats_columns: HashSet::new(),
            is_catalog_managed: false,
            skip_row_transforms: false,
        });
        let processor = ScanLogReplayProcessor::new(
            &engine,
            state_info,
            checkpoint_info.clone(),
            ScanStatsOptions::default(),
            ScanPartitionValuesOptions::default(),
        )
        .unwrap();
        let serialized = processor.into_serializable_state().unwrap();
        assert!(serialized.predicate.is_none());
        let deserialized =
            ScanLogReplayProcessor::from_serializable_state(&engine, serialized).unwrap();
        assert_eq!(deserialized.seen_file_keys.len(), 0);
        assert!(deserialized.state_info.transform_spec.is_none());
    }

    #[test]
    fn test_serialization_round_trips_is_catalog_managed() {
        let engine = SyncEngine::new();
        let schema: SchemaRef = schema_ref! { nullable "id": INTEGER };
        let state_info = Arc::new(StateInfo {
            logical_schema: schema.clone(),
            physical_schema: schema,
            physical_predicate: PhysicalPredicate::None,
            transform_spec: None,
            column_mapping_mode: ColumnMappingMode::None,
            physical_stats_schema: None,
            physical_partition_schema: None,
            physical_stats_columns: HashSet::new(),
            is_catalog_managed: true,
            skip_row_transforms: false,
        });
        let processor = ScanLogReplayProcessor::new(
            &engine,
            state_info,
            test_checkpoint_info(),
            ScanStatsOptions::default(),
            ScanPartitionValuesOptions::default(),
        )
        .unwrap();
        let serialized = processor.into_serializable_state().unwrap();
        let deserialized =
            ScanLogReplayProcessor::from_serializable_state(&engine, serialized).unwrap();
        assert!(deserialized.is_catalog_managed());
    }

    #[rstest]
    fn test_serialization_round_trips_skip_row_transforms(#[values(false, true)] skip: bool) {
        let engine = SyncEngine::new();
        let schema: SchemaRef = Arc::new(StructType::new_unchecked([StructField::new(
            "id",
            DataType::INTEGER,
            true,
        )]));
        let state_info = Arc::new(StateInfo {
            logical_schema: schema.clone(),
            physical_schema: schema.clone(),
            physical_predicate: PhysicalPredicate::None,
            transform_spec: None,
            column_mapping_mode: ColumnMappingMode::None,
            physical_stats_schema: None,
            physical_partition_schema: None,
            physical_stats_columns: HashSet::new(),
            is_catalog_managed: false,
            skip_row_transforms: skip,
        });
        let processor = ScanLogReplayProcessor::new(
            &engine,
            state_info,
            test_checkpoint_info(),
            ScanStatsOptions::default(),
            ScanPartitionValuesOptions::default(),
        )
        .unwrap();
        let deserialized = ScanLogReplayProcessor::from_serializable_state(
            &engine,
            processor.into_serializable_state().unwrap(),
        )
        .unwrap();
        assert_eq!(deserialized.state_info.skip_row_transforms, skip);
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
        let schema: SchemaRef = schema_ref! { nullable "id": INTEGER };
        let checkpoint_info = test_checkpoint_info();
        let invalid_internal_state = InternalScanState {
            logical_schema: schema.clone(),
            physical_schema: schema,
            predicate_schema: None, // Missing!
            transform_spec: None,
            column_mapping_mode: ColumnMappingMode::None,
            physical_stats_schema: None,
            stats_options: ScanStatsOptions::default(),
            partition_values_options: ScanPartitionValuesOptions::default(),
            physical_partition_schema: None,
            physical_stats_columns: HashSet::new(),
            is_catalog_managed: false,
            skip_row_transforms: false,
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
        let schema: SchemaRef = schema_ref! { nullable "id": INTEGER };
        let invalid_internal_state = InternalScanState {
            logical_schema: schema.clone(),
            physical_schema: schema,
            predicate_schema: None,
            transform_spec: None,
            column_mapping_mode: ColumnMappingMode::None,
            physical_stats_schema: None,
            stats_options: ScanStatsOptions::default(),
            partition_values_options: ScanPartitionValuesOptions::default(),
            physical_partition_schema: None,
            physical_stats_columns: HashSet::new(),
            is_catalog_managed: false,
            skip_row_transforms: false,
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

        let (iter, _metrics) = scan_action_iter(
            &SyncEngine::new(),
            batch
                .into_iter()
                .map(|batch| Ok(ActionsBatch::new(batch as _, true))),
            Arc::new(state_info),
            test_checkpoint_info(),
            ScanStatsOptions {
                skip_stats: true,
                ..Default::default()
            },
            ScanPartitionValuesOptions::default(),
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

    /// Verify that Remove actions are not pruned by data skipping. The transform reads from
    /// `add.*` columns, so Remove rows produce null `stats_parsed` and `partitionValues_parsed`
    /// (Remove actions have their own `remove.partitionValues` and `remove.stats`, but those
    /// are not read by the transform). If a Remove were pruned, it would not be recorded in
    /// `seen_file_keys`, and a subsequent Add for the same path could incorrectly survive
    /// deduplication.
    ///
    /// Stats-based skipping is safe because null stats evaluate to NULL via ISNULL guards in
    /// the predicate construction. Partition-based skipping requires the `is_add` guard
    /// (`OR(NOT is_add, pred)`) because `eval_sql_where` adds IS NOT NULL guards that would
    /// otherwise turn null partition values into `false`, filtering the Remove.
    #[rstest]
    #[case::stats_only(
        Arc::new(StructType::new_unchecked([
            StructField::new("value", DataType::INTEGER, true),
        ])),
        vec![],
        Arc::new(Expression::column(["value"]).gt(Expression::literal(5i32))),
        false, // use batch without partition column
    )]
    #[case::partition_predicate(
        Arc::new(StructType::new_unchecked([
            StructField::new("value", DataType::INTEGER, true),
            StructField::new("date", DataType::DATE, true),
        ])),
        vec!["date".to_string()],
        Arc::new(Expression::column(["date"]).eq(Expression::literal(Scalar::Date(17_510)))),
        true, // use batch with partition column
    )]
    #[case::mixed_stats_and_partition(
        Arc::new(StructType::new_unchecked([
            StructField::new("value", DataType::INTEGER, true),
            StructField::new("date", DataType::DATE, true),
        ])),
        vec!["date".to_string()],
        Arc::new(Predicate::and(
            Expression::column(["value"]).gt(Expression::literal(5i32)),
            Expression::column(["date"]).eq(Expression::literal(Scalar::Date(17_510))),
        )),
        true, // use batch with partition column
    )]
    fn data_skipping_does_not_prune_remove_actions(
        #[case] schema: SchemaRef,
        #[case] partition_columns: Vec<String>,
        #[case] predicate: Arc<Predicate>,
        #[case] with_partition: bool,
    ) {
        let state_info = get_state_info(
            schema,
            partition_columns,
            Some(predicate),
            &[],
            HashMap::new(),
            vec![],
        )
        .unwrap();

        // Batch: [Remove c001, Add c001, Add c000, Metadata]
        // The Remove must not be pruned -- it records c001 as seen, suppressing the c001 Add.
        let batch = if with_partition {
            vec![add_batch_with_remove_and_partition(
                get_commit_schema().clone(),
            )]
        } else {
            vec![add_batch_with_remove(get_commit_schema().clone())]
        };
        let (iter, _metrics) = scan_action_iter(
            &SyncEngine::new(),
            batch
                .into_iter()
                .map(|batch| Ok(ActionsBatch::new(batch as _, true))),
            Arc::new(state_info),
            test_checkpoint_info(),
            ScanStatsOptions::default(),
            ScanPartitionValuesOptions::default(),
        )
        .unwrap();

        let mut add_paths: Vec<String> = Vec::new();
        for res in iter {
            let scan_metadata = res.unwrap();
            let paths = scan_metadata
                .visit_scan_files(
                    Vec::new(),
                    |paths: &mut Vec<String>, scan_file: ScanFile| {
                        paths.push(scan_file.path.to_string());
                    },
                )
                .unwrap();
            add_paths.extend(paths);
        }

        // Only c000 should survive: Remove suppressed c001 via deduplication
        assert_eq!(add_paths.len(), 1, "Expected exactly one add to survive");
        assert!(
            add_paths[0].contains("c000"),
            "Expected c000 add to survive, got: {}",
            add_paths[0]
        );
    }

    /// Walk `expr` and count `Unary { op: ToJson, .. }` occurrences anywhere in the tree.
    // Closures wrap `count_to_json` to dereference `&Arc<Expression>` -> `&Expression`;
    // clippy doesn't see the auto-deref so flags them as redundant.
    #[allow(clippy::redundant_closure)]
    fn count_to_json(expr: &Expression) -> usize {
        match expr {
            Expression::Unary(u) => {
                let here = (u.op == UnaryExpressionOp::ToJson) as usize;
                here + count_to_json(&u.expr)
            }
            Expression::Binary(b) => count_to_json(&b.left) + count_to_json(&b.right),
            Expression::Variadic(v) => v.exprs.iter().map(|e| count_to_json(e)).sum(),
            Expression::Struct(fields, nullability) => {
                fields.iter().map(|e| count_to_json(e)).sum::<usize>()
                    + nullability.as_ref().map_or(0, |e| count_to_json(e))
            }
            Expression::StructPatch(p) => {
                p.prepended_fields
                    .iter()
                    .chain(p.appended_fields.iter())
                    .map(|e| count_to_json(e))
                    .sum::<usize>()
                    + p.field_patches
                        .values()
                        .flat_map(|fp| fp.insertions.iter())
                        .map(|e| count_to_json(e))
                        .sum::<usize>()
            }
            Expression::ParseJson(p) => count_to_json(&p.json_expr),
            Expression::MapToStruct(m) => count_to_json(&m.map_expr),
            Expression::Predicate(_)
            | Expression::Literal(_)
            | Expression::Column(_)
            | Expression::Opaque(_)
            | Expression::Unknown(_) => 0,
        }
    }

    /// `synthesize_json=false` removes every `ToJson` node from the add transform;
    /// `synthesize_json=true` leaves exactly one inside the COALESCE branch.
    #[test]
    fn add_transform_omits_to_json_when_synthesis_skipped() {
        let stats_schema: SchemaRef = Arc::new(StructType::new_unchecked([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("value", DataType::STRING),
        ]));
        let partition_schema: Option<SchemaRef> = Some(Arc::new(StructType::new_unchecked([
            StructField::nullable("date", DataType::DATE),
        ])));

        // Synthesis enabled: COALESCE branch present -> exactly one ToJson.
        let with_synthesis = get_add_transform_expr(
            Some(stats_schema.clone()),
            true,  // has_stats_parsed
            false, // skip_stats
            true,  // synthesize_json
            partition_schema.clone(),
            false, // has_partition_values_parsed
        );
        assert_eq!(
            count_to_json(&with_synthesis),
            1,
            "expected exactly one ToJson(add.stats_parsed) in the COALESCE branch when synthesis is enabled"
        );

        // Synthesis disabled: no ToJson anywhere in the transform.
        let without_synthesis = get_add_transform_expr(
            Some(stats_schema),
            true,  // has_stats_parsed
            false, // skip_stats
            false, // synthesize_json
            partition_schema,
            false, // has_partition_values_parsed
        );
        assert_eq!(
            count_to_json(&without_synthesis),
            0,
            "expected no ToJson nodes anywhere in the transform when synthesis is skipped"
        );
    }

    /// `partition_schema_has_zoned_timestamp` gates the native-checkpoint reparse: it is true only
    /// when a zoned `TIMESTAMP` column is present. `TIMESTAMP_NTZ`, date, and other types are
    /// zone-independent and keep the fast path.
    #[test]
    fn partition_schema_has_zoned_timestamp_only_for_tz_timestamp() {
        use crate::schema::PrimitiveType;

        let ts: SchemaRef = Arc::new(StructType::new_unchecked([StructField::nullable(
            "p",
            DataType::TIMESTAMP,
        )]));
        assert!(partition_schema_has_zoned_timestamp(Some(&ts)));

        let mixed: SchemaRef = Arc::new(StructType::new_unchecked([
            StructField::nullable("d", DataType::DATE),
            StructField::nullable("n", DataType::Primitive(PrimitiveType::TimestampNtz)),
            StructField::nullable("t", DataType::TIMESTAMP),
        ]));
        assert!(partition_schema_has_zoned_timestamp(Some(&mixed)));

        let no_ts: SchemaRef = Arc::new(StructType::new_unchecked([
            StructField::nullable("d", DataType::DATE),
            StructField::nullable("n", DataType::Primitive(PrimitiveType::TimestampNtz)),
            StructField::nullable("i", DataType::INTEGER),
        ]));
        assert!(!partition_schema_has_zoned_timestamp(Some(&no_ts)));
        assert!(!partition_schema_has_zoned_timestamp(None));
    }

    /// `annotate_partition_timezone` stamps the session zone only on `TIMESTAMP` leaves, leaving
    /// zone-independent columns untouched, and is a no-op when no zone is supplied.
    #[test]
    fn annotate_partition_timezone_stamps_only_timestamp_leaves() {
        use crate::schema::{ColumnMetadataKey, PrimitiveType};

        let schema = StructType::new_unchecked([
            StructField::nullable("t", DataType::TIMESTAMP),
            StructField::nullable("n", DataType::Primitive(PrimitiveType::TimestampNtz)),
            StructField::nullable("d", DataType::DATE),
        ]);
        let tz_key = ColumnMetadataKey::SessionTimezone.as_ref();

        let annotated = annotate_partition_timezone(&schema, Some("America/New_York"));
        let by_name = |name: &str| {
            annotated
                .field(name)
                .unwrap()
                .metadata()
                .contains_key(tz_key)
        };
        assert!(by_name("t"), "TIMESTAMP leaf must carry the zone");
        assert!(!by_name("n"), "TIMESTAMP_NTZ is zone-independent");
        assert!(!by_name("d"), "DATE is zone-independent");

        // No zone: schema passes through unstamped.
        let unstamped = annotate_partition_timezone(&schema, None);
        assert!(!unstamped
            .field("t")
            .unwrap()
            .metadata()
            .contains_key(tz_key));
    }
}
