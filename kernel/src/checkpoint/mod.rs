//! This module implements the API for writing single-file checkpoints.
//!
//! The entry point for this API is [`Snapshot::create_checkpoint_writer`].
//!
//! ## Checkpoint Types and Selection Logic
//! This API supports two checkpoint types, selected based on table features:
//!
//! | Table Feature    | Resulting Checkpoint Type    | Description                                                                 |
//! |------------------|-------------------------------|-----------------------------------------------------------------------------|
//! | No v2Checkpoints | Single-file Classic-named V1 | Follows V1 specification without [`CheckpointMetadata`] action             |
//! | v2Checkpoints    | Single-file Classic-named V2 | Follows V2 specification with [`CheckpointMetadata`] action while maintaining backward compatibility via classic naming |
//!
//! For more information on the V1/V2 specifications, see the following protocol section:
//! <https://github.com/delta-io/delta/blob/master/PROTOCOL.md#checkpoint-specs>
//!
//! ## Architecture
//!
//! - [`CheckpointWriter`] - Core component that manages the checkpoint creation workflow
//! - [`ActionReconciliationIterator`] - Iterator over the checkpoint data to be written
//!
//! ## Usage
//!
//! The following steps outline the process of creating a checkpoint:
//!
//! 1. Create a [`CheckpointWriter`] using [`Snapshot::create_checkpoint_writer`]
//! 2. Get the checkpoint path from [`CheckpointWriter::checkpoint_path`]
//! 2. Get the checkpoint data from [`CheckpointWriter::checkpoint_data`]
//! 3. Write the data to the path in object storage (engine-specific)
//! 4. Collect metadata ([`FileMeta`]) from the write operation
//! 5. Pass the metadata and exhausted data iterator to [`CheckpointWriter::finalize`]
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use delta_kernel::ActionReconciliationIterator;
//! # use delta_kernel::checkpoint::CheckpointWriter;
//! # use delta_kernel::Engine;
//! # use delta_kernel::Snapshot;
//! # use delta_kernel::SnapshotRef;
//! # use delta_kernel::DeltaResult;
//! # use delta_kernel::Error;
//! # use delta_kernel::FileMeta;
//! # use url::Url;
//! fn write_checkpoint_file(path: Url, data: &mut ActionReconciliationIterator) -> DeltaResult<FileMeta> {
//!     todo!() /* engine-specific logic to write data to object storage*/
//! }
//!
//! let engine: &dyn Engine = todo!(); /* create engine instance */
//!
//! // Create a snapshot for the table at the version you want to checkpoint
//! let url = delta_kernel::try_parse_uri("./tests/data/app-txn-no-checkpoint")?;
//! let snapshot = Snapshot::builder_for(url).build(engine)?;
//!
//! // Create a checkpoint writer from the snapshot
//! let mut writer = snapshot.create_checkpoint_writer()?;
//!
//! // Get the checkpoint path and data
//! let checkpoint_path = writer.checkpoint_path()?;
//! let checkpoint_data = writer.checkpoint_data(engine)?;
//!
//! // Get the iterator state
//! let state = checkpoint_data.state();
//!
//! // Write the checkpoint data to the object store and collect metadata
//! let metadata: FileMeta = write_checkpoint_file(checkpoint_path, &checkpoint_data)?;
//!
//! /* IMPORTANT: All data must be written before finalizing the checkpoint */
//!
//! // Finalize the checkpoint by passing the metadata and state handle
//! writer.finalize(engine, &metadata, &state)?;
//!
//! # Ok::<_, Error>(())
//! ```
//!
//! ## Warning
//! Multi-part (V1) checkpoints are DEPRECATED and UNSAFE.
//!
//! ## Note
//! We currently do not plan to support UUID-named V2 checkpoints, since S3's put-if-absent
//! semantics remove the need for UUIDs to ensure uniqueness. Supporting only classic-named
//! checkpoints avoids added complexity, such as coordinating naming decisions between kernel and
//! engine, and handling coexistence with legacy V1 checkpoints. If a compelling use case arises
//! in the future, we can revisit this decision.
//!
//! [`CheckpointMetadata`]: crate::actions::CheckpointMetadata
//! [`LastCheckpointHint`]: crate::last_checkpoint_hint::LastCheckpointHint
//! [`Snapshot::create_checkpoint_writer`]: crate::Snapshot::create_checkpoint_writer
// Future extensions:
// - TODO(#837): Multi-file V2 checkpoints are not supported yet. The API is designed to be extensible for future
//   multi-file support, but the current implementation only supports single-file checkpoints.
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use crate::action_reconciliation::log_replay::{
    ActionReconciliationBatch, ActionReconciliationProcessor,
};
use crate::action_reconciliation::{
    ActionReconciliationIterator, ActionReconciliationIteratorState, RetentionCalculator,
};
use crate::actions::{
    Add, DomainMetadata, Metadata, Protocol, Remove, SetTransaction, Sidecar, ADD_NAME,
    CHECKPOINT_METADATA_NAME, DOMAIN_METADATA_NAME, METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME,
    SET_TRANSACTION_NAME, SIDECAR_NAME,
};
use crate::engine_data::FilteredEngineData;
use crate::expressions::{Expression, Scalar, StructData, Transform};
use crate::last_checkpoint_hint::LastCheckpointHint;
use crate::log_replay::LogReplayProcessor;
use crate::metrics::{MetricEvent, MetricId};
use crate::path::ParsedLogPath;
use crate::schema::{DataType, SchemaRef, StructField, StructType, ToSchema as _};
use crate::snapshot::SnapshotRef;
use crate::table_features::TableFeature;
use crate::table_properties::TableProperties;
use crate::{DeltaResult, Engine, EngineData, Error, EvaluationHandlerExtension, FileMeta};

use tracing::instrument;
use url::Url;

mod stats_transform;

use stats_transform::{
    build_checkpoint_output_schema, build_checkpoint_read_schema_with_stats, build_stats_transform,
    StatsTransformConfig,
};

#[cfg(test)]
mod tests;

/// Schema of the `_last_checkpoint` file
/// We cannot use `LastCheckpointInfo::to_schema()` as it would include the 'checkpoint_schema'
/// field, which is only known at runtime.
static LAST_CHECKPOINT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    StructType::new_unchecked([
        StructField::not_null("version", DataType::LONG),
        StructField::not_null("size", DataType::LONG),
        StructField::nullable("parts", DataType::LONG),
        StructField::nullable("sizeInBytes", DataType::LONG),
        StructField::nullable("numOfAddFiles", DataType::LONG),
    ])
    .into()
});

/// Action fields shared by V1 and V2 checkpoint schemas.
fn base_checkpoint_action_fields() -> Vec<StructField> {
    vec![
        StructField::nullable(ADD_NAME, Add::to_schema()),
        StructField::nullable(REMOVE_NAME, Remove::to_schema()),
        StructField::nullable(METADATA_NAME, Metadata::to_schema()),
        StructField::nullable(PROTOCOL_NAME, Protocol::to_schema()),
        StructField::nullable(SET_TRANSACTION_NAME, SetTransaction::to_schema()),
        StructField::nullable(DOMAIN_METADATA_NAME, DomainMetadata::to_schema()),
        StructField::nullable(SIDECAR_NAME, Sidecar::to_schema()),
    ]
}

/// Schema for V1 checkpoints (without checkpointMetadata action)
static CHECKPOINT_ACTIONS_SCHEMA_V1: LazyLock<SchemaRef> =
    LazyLock::new(|| Arc::new(StructType::new_unchecked(base_checkpoint_action_fields())));

/// Schema for the checkpointMetadata field in V2 checkpoints.
/// We cannot use `CheckpointMetadata::to_schema()` as it would include the 'tags' field which
/// we're not supporting yet due to the lack of map support TODO(#880).
fn checkpoint_metadata_field() -> StructField {
    StructField::nullable(
        CHECKPOINT_METADATA_NAME,
        DataType::struct_type_unchecked([StructField::not_null("version", DataType::LONG)]),
    )
}

/// Schema for V2 checkpoints (includes checkpointMetadata action)
static CHECKPOINT_ACTIONS_SCHEMA_V2: LazyLock<SchemaRef> = LazyLock::new(|| {
    let mut fields = base_checkpoint_action_fields();
    fields.push(checkpoint_metadata_field());
    Arc::new(StructType::new_unchecked(fields))
});

/// Orchestrates the process of creating a checkpoint for a table.
///
/// The [`CheckpointWriter`] is the entry point for generating checkpoint data for a Delta table.
/// It automatically selects the appropriate checkpoint format (V1/V2) based on whether the table
/// supports the `v2Checkpoints` reader/writer feature.
///
/// # Warning
/// The checkpoint data must be fully written to storage before calling [`CheckpointWriter::finalize`].
/// Failing to do so may result in data loss or corruption.
///
/// # See Also
/// See the [module-level documentation](self) for the complete checkpoint workflow
#[derive(Debug)]
pub struct CheckpointWriter {
    /// Reference to the snapshot (i.e. version) of the table being checkpointed
    pub(crate) snapshot: SnapshotRef,

    /// The version of the snapshot being checkpointed.
    /// Note: Although the version is stored as a u64 in the snapshot, it is stored as an i64
    /// field here to avoid multiple type conversions.
    version: i64,

    /// Cumulative nanoseconds spent evaluating stats transforms across all batches.
    /// Shared with the lazy iterator returned by `checkpoint_data()`.
    stats_transform_nanos: Arc<AtomicU64>,

    /// Duration of the setup phase in `checkpoint_data()` (schema building, evaluator creation).
    /// Set once when `checkpoint_data()` completes.
    setup_duration: Arc<AtomicU64>,
}

impl RetentionCalculator for CheckpointWriter {
    fn table_properties(&self) -> &TableProperties {
        self.snapshot.table_properties()
    }
}

impl CheckpointWriter {
    /// Creates a new [`CheckpointWriter`] for the given snapshot.
    pub(crate) fn try_new(snapshot: SnapshotRef) -> DeltaResult<Self> {
        let version = i64::try_from(snapshot.version()).map_err(|e| {
            Error::CheckpointWrite(format!(
                "Failed to convert checkpoint version from u64 {} to i64: {}",
                snapshot.version(),
                e
            ))
        })?;

        // We disallow checkpointing if the Snapshot is not published. If we didn't, this could
        // create gaps in the version history, thereby breaking old readers.
        snapshot.log_segment().validate_published()?;

        Ok(Self {
            snapshot,
            version,
            stats_transform_nanos: Arc::new(AtomicU64::new(0)),
            setup_duration: Arc::new(AtomicU64::new(0)),
        })
    }
    /// Returns the URL where the checkpoint file should be written.
    ///
    /// This method generates the checkpoint path based on the table's root and the version
    /// of the underlying snapshot being checkpointed. The resulting path follows the classic
    /// Delta checkpoint naming convention (where the version is zero-padded to 20 digits):
    ///
    /// `<table_root>/<version>.checkpoint.parquet`
    ///
    /// For example, if the table root is `s3://bucket/path` and the version is `10`,
    /// the checkpoint path will be: `s3://bucket/path/00000000000000000010.checkpoint.parquet`
    pub fn checkpoint_path(&self) -> DeltaResult<Url> {
        ParsedLogPath::new_classic_parquet_checkpoint(
            self.snapshot.table_root(),
            self.snapshot.version(),
        )
        .map(|parsed| parsed.location)
    }
    /// Returns the checkpoint data to be written to the checkpoint file.
    ///
    /// This method reads actions from the log segment, processes them for checkpoint creation,
    /// and applies stats transforms based on table properties:
    /// - `delta.checkpoint.writeStatsAsJson` (default: true)
    /// - `delta.checkpoint.writeStatsAsStruct` (default: false)
    ///
    /// The returned [`ActionReconciliationIterator`] yields [`FilteredEngineData`] batches with
    /// stats transforms already applied. Use [`ActionReconciliationIterator::state`] to get the
    /// shared state for passing to [`CheckpointWriter::finalize`].
    ///
    /// # Engine Usage
    ///
    /// ```ignore
    /// let mut checkpoint_data = writer.checkpoint_data(&engine)?;
    /// let state = checkpoint_data.state();
    /// while let Some(batch) = checkpoint_data.next() {
    ///     let data = batch?.apply_selection_vector()?;
    ///     parquet_writer.write(&data).await?;
    /// }
    /// writer.finalize(&engine, &metadata, &state)?;
    /// ```
    // Implementation overview:
    // 1. Determines whether to write a V1 or V2 checkpoint based on `v2Checkpoints` feature
    // 2. Builds a read schema with stats_parsed for COALESCE expressions
    // 3. Reads actions from the log segment and deduplicates via reconciliation
    // 4. Applies stats transforms (COALESCE/drop) to each reconciled batch
    // 5. Chains the checkpoint metadata action for V2 checkpoints
    #[instrument(
        name = "checkpoint.data",
        skip_all,
        fields(version = self.version),
        err
    )]
    pub fn checkpoint_data(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<ActionReconciliationIterator> {
        let setup_start = Instant::now();

        let config = StatsTransformConfig::from_table_properties(self.snapshot.table_properties());

        // Get clustering columns so they are always included in stats per the Delta protocol.
        let clustering_columns = self.snapshot.get_clustering_columns(engine)?;

        // Get stats schema from table configuration.
        // This already excludes partition columns and applies column mapping.
        let stats_schema = self
            .snapshot
            .table_configuration()
            .build_expected_stats_schemas(clustering_columns.as_deref())?
            .physical;

        // Select schema based on V2 checkpoint support
        let is_v2_checkpoints_supported = self
            .snapshot
            .table_configuration()
            .is_feature_supported(&TableFeature::V2Checkpoint);

        let base_schema = if is_v2_checkpoints_supported {
            &CHECKPOINT_ACTIONS_SCHEMA_V2
        } else {
            &CHECKPOINT_ACTIONS_SCHEMA_V1
        };

        // The read schema and output schema differ because the transform needs access to
        // both stats formats as input, but may only write one format as output.
        //
        // read_schema: Always includes both `stats` and `stats_parsed` fields in the Add
        // action, so COALESCE expressions can read from either source. For commit files,
        // `stats_parsed` doesn't exist and is read as nulls.
        //
        // output_schema: Only includes the stats fields that the table config requests
        // (e.g., only `stats` if writeStatsAsJson=true and writeStatsAsStruct=false).
        let read_schema = build_checkpoint_read_schema_with_stats(base_schema, &stats_schema)?;
        let output_schema = build_checkpoint_output_schema(&config, base_schema, &stats_schema)?;

        // Read actions from log segment
        let actions =
            self.snapshot
                .log_segment()
                .read_actions(engine, read_schema.clone(), None)?;

        // Process actions through reconciliation
        let checkpoint_data = ActionReconciliationProcessor::new(
            self.deleted_file_retention_timestamp()?,
            self.get_transaction_expiration_timestamp()?,
        )
        .process_actions_iter(actions);

        // Build transform expression and create expression evaluator.
        // The transform is applied to reconciled action batches only (not checkpoint metadata).
        let transform_expr = build_stats_transform(&config, &stats_schema);
        let evaluator = engine.evaluation_handler().new_expression_evaluator(
            read_schema,
            transform_expr,
            output_schema.clone().into(),
        )?;

        let setup_nanos = setup_start.elapsed().as_nanos() as u64;
        self.setup_duration.store(setup_nanos, Ordering::Release);

        // Apply stats transform to each reconciled batch, accumulating transform time
        let transform_nanos = Arc::clone(&self.stats_transform_nanos);
        let transformed = checkpoint_data.map(move |batch_result| {
            let batch = batch_result?;
            let (data, sv) = batch.filtered_data.into_parts();
            let transform_start = Instant::now();
            let transformed = evaluator.evaluate(data.as_ref())?;
            transform_nanos.fetch_add(
                transform_start.elapsed().as_nanos() as u64,
                Ordering::Release,
            );
            Ok(ActionReconciliationBatch {
                filtered_data: FilteredEngineData::try_new(transformed, sv)?,
                actions_count: batch.actions_count,
                add_actions_count: batch.add_actions_count,
            })
        });

        // For V2 checkpoints, chain the checkpoint metadata batch after the transformed
        // action stream. The metadata batch is created with the output schema directly,
        // bypassing the stats transform (it has no add actions to transform).
        let checkpoint_metadata = is_v2_checkpoints_supported
            .then(|| self.create_checkpoint_metadata_batch(engine, &output_schema));

        Ok(ActionReconciliationIterator::new(Box::new(
            transformed.chain(checkpoint_metadata),
        )))
    }

    /// Finalizes checkpoint creation by saving metadata about the checkpoint.
    ///
    /// # Important
    /// This method **must** be called only after:
    /// 1. The checkpoint data iterator has been fully exhausted
    /// 2. All data has been successfully written to object storage
    ///
    /// # Parameters
    /// - `engine`: Implementation of [`Engine`] apis.
    /// - `metadata`: The metadata of the written checkpoint file
    /// - `checkpoint_iter_state`: The state of the checkpoint data iterator
    ///
    /// # Returns: `Ok` if the checkpoint was successfully finalized
    // Internally, this method:
    // 1. Validates that the checkpoint data iterator is fully exhausted
    // 2. Creates the `_last_checkpoint` data with `create_last_checkpoint_data`
    // 3. Writes the `_last_checkpoint` data to the `_last_checkpoint` file in the delta log
    /// Reports metrics: `CheckpointWriteCompleted`.
    #[instrument(
        name = "checkpoint.finalize",
        skip_all,
        fields(version = self.version),
        err
    )]
    pub fn finalize(
        self,
        engine: &dyn Engine,
        metadata: &FileMeta,
        checkpoint_iter_state: &ActionReconciliationIteratorState,
    ) -> DeltaResult<()> {
        let start = Instant::now();
        let reporter = engine.get_metrics_reporter();

        // Ensure the checkpoint data iterator is fully exhausted
        if !checkpoint_iter_state.is_exhausted() {
            return Err(Error::checkpoint_write(
                "The checkpoint data iterator must be fully consumed and written to storage before calling finalize"
            ));
        }

        let size_in_bytes = i64::try_from(metadata.size).map_err(|e| {
            Error::CheckpointWrite(format!(
                "Failed to convert checkpoint size in bytes from u64 {} to i64: {}, when writing _last_checkpoint",
                metadata.size, e
            ))
        })?;

        let data = create_last_checkpoint_data(
            engine,
            self.version,
            checkpoint_iter_state.actions_count(),
            checkpoint_iter_state.add_actions_count(),
            size_in_bytes,
        );

        let last_checkpoint_path = LastCheckpointHint::path(&self.snapshot.log_segment().log_root)?;

        // Write the `_last_checkpoint` file to `table/_delta_log/_last_checkpoint`
        let filtered_data = FilteredEngineData::with_all_rows_selected(data?);
        engine.json_handler().write_json_file(
            &last_checkpoint_path,
            Box::new(std::iter::once(Ok(filtered_data))),
            true,
        )?;

        let config = StatsTransformConfig::from_table_properties(self.snapshot.table_properties());
        let setup_nanos = self.setup_duration.load(Ordering::Acquire);
        let transform_nanos = self.stats_transform_nanos.load(Ordering::Acquire);
        reporter.as_ref().inspect(|r| {
            r.report(MetricEvent::CheckpointWriteCompleted {
                operation_id: MetricId::new(),
                duration: start.elapsed(),
                version: self.snapshot.version(),
                write_stats_as_json: config.write_stats_as_json,
                write_stats_as_struct: config.write_stats_as_struct,
                setup_duration: Duration::from_nanos(setup_nanos),
                stats_transform_duration: Duration::from_nanos(transform_nanos),
            });
        });

        Ok(())
    }

    /// Creates the checkpoint metadata action for V2 checkpoints.
    ///
    /// This function generates the [`CheckpointMetadata`] action that must be included in the
    /// V2 spec checkpoint file. This action contains metadata about the checkpoint, particularly
    /// its version.
    ///
    /// # Implementation Details
    ///
    /// The function creates a single-row [`EngineData`] batch using the output checkpoint
    /// schema, with all action fields (add, remove, etc.) set to null except for the
    /// `checkpointMetadata` field. This ensures the checkpoint metadata batch has the same
    /// schema as other action batches, allowing them to be written to the same Parquet file.
    ///
    /// The batch is created directly with the output schema and does not go through the stats
    /// transform pipeline, since it contains no `add` actions to transform.
    ///
    /// # Returns:
    /// An [`ActionReconciliationBatch`] including the single-row [`EngineData`] batch along with
    /// an accompanying selection vector with a single `true` value, indicating the action in
    /// the batch should be included in the checkpoint.
    fn create_checkpoint_metadata_batch(
        &self,
        engine: &dyn Engine,
        schema: &SchemaRef,
    ) -> DeltaResult<ActionReconciliationBatch> {
        // Start with an all-null row
        let null_row = engine.evaluation_handler().null_row(schema.clone())?;

        // Build the checkpointMetadata struct value
        let checkpoint_metadata_value = Scalar::Struct(StructData::try_new(
            vec![StructField::not_null("version", DataType::LONG)],
            vec![Scalar::from(self.version)],
        )?);

        // Use a Transform to set just the checkpointMetadata field, keeping others null
        let transform = Transform::new_top_level().with_replaced_field(
            CHECKPOINT_METADATA_NAME,
            Arc::new(Expression::literal(checkpoint_metadata_value)),
        );

        let evaluator = engine.evaluation_handler().new_expression_evaluator(
            schema.clone(),
            Arc::new(Expression::transform(transform)),
            schema.clone().into(),
        )?;

        let checkpoint_metadata_batch = evaluator.evaluate(null_row.as_ref())?;

        let filtered_data = FilteredEngineData::with_all_rows_selected(checkpoint_metadata_batch);

        Ok(ActionReconciliationBatch {
            filtered_data,
            actions_count: 1,
            add_actions_count: 0,
        })
    }
}

/// Creates the data for the _last_checkpoint file containing checkpoint
/// metadata with the `create_one` method. Factored out to facilitate testing.
///
/// # Parameters
/// - `engine`: Engine for data processing
/// - `version`: Table version number
/// - `actions_counter`: Total actions count
/// - `add_actions_counter`: Add actions count
/// - `size_in_bytes`: Size of the checkpoint file in bytes
///
/// # Returns
/// A new [`EngineData`] batch with the `_last_checkpoint` fields:
/// - `version` (i64, required): Table version number
/// - `size` (i64, required): Total actions count
/// - `parts` (i64, optional): Always None for single-file checkpoints
/// - `sizeInBytes` (i64, optional): Size of checkpoint file in bytes
/// - `numOfAddFiles` (i64, optional): Number of Add actions
///
/// TODO(#838): Add `checksum` field to `_last_checkpoint` file
/// TODO(#839): Add `checkpoint_schema` field to `_last_checkpoint` file
/// TODO(#1054): Add `tags` field to `_last_checkpoint` file
/// TODO(#1052): Add `v2Checkpoint` field to `_last_checkpoint` file
pub(crate) fn create_last_checkpoint_data(
    engine: &dyn Engine,
    version: i64,
    actions_counter: i64,
    add_actions_counter: i64,
    size_in_bytes: i64,
) -> DeltaResult<Box<dyn EngineData>> {
    engine.evaluation_handler().create_one(
        LAST_CHECKPOINT_SCHEMA.clone(),
        &[
            version.into(),
            actions_counter.into(),
            None::<i64>.into(), // parts = None since we only support single-part checkpoints
            size_in_bytes.into(),
            add_actions_counter.into(),
        ],
    )
}
