//! This module implements the API for writing single-file checkpoints.
//!
//! The entry point for this API is [`Snapshot::checkpoint`].
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
//! 1. Create a [`CheckpointWriter`] using [`Snapshot::checkpoint`]
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
//! # use delta_kernel::checkpoint::TransformingCheckpointIterator;
//! # use delta_kernel::Engine;
//! # use delta_kernel::Snapshot;
//! # use delta_kernel::SnapshotRef;
//! # use delta_kernel::DeltaResult;
//! # use delta_kernel::Error;
//! # use delta_kernel::FileMeta;
//! # use url::Url;
//! fn write_checkpoint_file(path: Url, data: &TransformingCheckpointIterator) -> DeltaResult<FileMeta> {
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
//! let mut writer = snapshot.checkpoint()?;
//!
//! // Get the checkpoint path and data
//! let checkpoint_path = writer.checkpoint_path()?;
//! let checkpoint_data = writer.checkpoint_data(engine)?;
//!
//! // Write the checkpoint data to the object store and collect metadata
//! let metadata: FileMeta = write_checkpoint_file(checkpoint_path, &checkpoint_data)?;
//!
//! /* IMPORTANT: All data must be written before finalizing the checkpoint */
//!
//! // Finalize the checkpoint by passing the metadata and exhausted data iterator
//! writer.finalize(engine, &metadata, checkpoint_data)?;
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
//! [`Snapshot::checkpoint`]: crate::Snapshot::checkpoint
// Future extensions:
// - TODO(#837): Multi-file V2 checkpoints are not supported yet. The API is designed to be extensible for future
//   multi-file support, but the current implementation only supports single-file checkpoints.
use std::sync::{Arc, LazyLock};

use crate::action_reconciliation::log_replay::{
    ActionReconciliationBatch, ActionReconciliationProcessor,
};
use crate::action_reconciliation::{ActionReconciliationIterator, RetentionCalculator};
use crate::actions::{
    Add, Metadata, Protocol, Remove, SetTransaction, Sidecar, ADD_NAME, CHECKPOINT_METADATA_NAME,
    METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME, SET_TRANSACTION_NAME, SIDECAR_NAME,
};
use crate::engine_data::FilteredEngineData;
use crate::expressions::Scalar;
use crate::last_checkpoint_hint::LastCheckpointHint;
use crate::log_replay::LogReplayProcessor;
use crate::path::ParsedLogPath;
use crate::schema::{DataType, SchemaRef, StructField, StructType, ToSchema as _};
use crate::snapshot::SnapshotRef;
use crate::table_features::TableFeature;
use crate::table_properties::TableProperties;
use crate::{DeltaResult, Engine, EngineData, Error, EvaluationHandlerExtension, FileMeta};

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

/// Schema for extracting relevant actions from log files for checkpoint creation
static CHECKPOINT_ACTIONS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new_unchecked([
        StructField::nullable(ADD_NAME, Add::to_schema()),
        StructField::nullable(REMOVE_NAME, Remove::to_schema()),
        StructField::nullable(METADATA_NAME, Metadata::to_schema()),
        StructField::nullable(PROTOCOL_NAME, Protocol::to_schema()),
        StructField::nullable(SET_TRANSACTION_NAME, SetTransaction::to_schema()),
        StructField::nullable(SIDECAR_NAME, Sidecar::to_schema()),
    ]))
});

// Schema of the [`CheckpointMetadata`] action that is included in V2 checkpoints
// We cannot use `CheckpointMetadata::to_schema()` as it would include the 'tags' field which
// we're not supporting yet due to the lack of map support TODO(#880).
static CHECKPOINT_METADATA_ACTION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new_unchecked([StructField::nullable(
        CHECKPOINT_METADATA_NAME,
        DataType::struct_type_unchecked([StructField::not_null("version", DataType::LONG)]),
    )]))
});

/// Iterator that applies stats transforms to checkpoint data batches.
///
/// This iterator wraps an [`ActionReconciliationIterator`] and applies an optional
/// expression evaluator to each batch to populate stats fields.
pub struct TransformingCheckpointIterator {
    inner: ActionReconciliationIterator,
    evaluator: Option<Arc<dyn crate::ExpressionEvaluator>>,
    /// Optional CheckpointMetadata batch to yield after all actions (for V2 checkpoints).
    /// This is kept separate because it has a different schema and shouldn't be transformed.
    checkpoint_metadata: Option<FilteredEngineData>,
    /// Track whether we had checkpoint metadata (for actions_count after consumption)
    has_checkpoint_metadata: bool,
    /// Schema for writing checkpoint data (includes/excludes stats fields based on config)
    output_schema: SchemaRef,
}

impl TransformingCheckpointIterator {
    /// Creates a new transforming iterator.
    pub(crate) fn new(
        inner: ActionReconciliationIterator,
        evaluator: Option<Arc<dyn crate::ExpressionEvaluator>>,
        checkpoint_metadata: Option<FilteredEngineData>,
        output_schema: SchemaRef,
    ) -> Self {
        let has_checkpoint_metadata = checkpoint_metadata.is_some();
        Self {
            inner,
            evaluator,
            checkpoint_metadata,
            has_checkpoint_metadata,
            output_schema,
        }
    }

    /// Returns the schema for writing checkpoint data.
    ///
    /// This schema reflects the table's stats configuration:
    /// - Includes `stats` field when `writeStatsAsJson=true`
    /// - Includes `stats_parsed` field when `writeStatsAsStruct=true`
    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }

    /// Returns true if the iterator has been fully consumed.
    pub fn is_exhausted(&self) -> bool {
        self.inner.is_exhausted() && self.checkpoint_metadata.is_none()
    }

    /// Returns the total count of actions processed.
    pub fn actions_count(&self) -> i64 {
        let metadata_count = if self.has_checkpoint_metadata { 1 } else { 0 };
        self.inner.actions_count() + metadata_count
    }

    /// Returns the count of add actions processed.
    pub fn add_actions_count(&self) -> i64 {
        self.inner.add_actions_count()
    }
}

impl Iterator for TransformingCheckpointIterator {
    type Item = DeltaResult<FilteredEngineData>;

    fn next(&mut self) -> Option<Self::Item> {
        // First, yield all transformed action batches
        if let Some(batch) = self.inner.next() {
            // If no evaluator, pass through unchanged
            let Some(evaluator) = &self.evaluator else {
                return Some(batch);
            };

            // Apply the transform to the batch
            return Some(batch.and_then(|filtered_data| {
                let (engine_data, selection_vector) = filtered_data.into_parts();
                let transformed = evaluator.evaluate(engine_data.as_ref())?;
                FilteredEngineData::try_new(transformed, selection_vector)
            }));
        }

        // After all actions, yield the checkpoint metadata batch (if any) unchanged
        self.checkpoint_metadata.take().map(Ok)
    }
}

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
#[derive(Debug, Clone)]
pub struct CheckpointWriter {
    /// Reference to the snapshot (i.e. version) of the table being checkpointed
    pub(crate) snapshot: SnapshotRef,

    /// The version of the snapshot being checkpointed.
    /// Note: Although the version is stored as a u64 in the snapshot, it is stored as an i64
    /// field here to avoid multiple type conversions.
    version: i64,
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

        // We disallow checkpointing if the LogSegment contains any unpublished commits. (could
        // create gaps in the version history, thereby breaking old readers)
        snapshot.log_segment().validate_no_staged_commits()?;

        Ok(Self { snapshot, version })
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
    /// The returned [`TransformingCheckpointIterator`] yields batches with stats transforms
    /// already applied. Use [`TransformingCheckpointIterator::output_schema`] to get the
    /// schema for writing the checkpoint file.
    ///
    /// # Engine Usage
    ///
    /// ```ignore
    /// let mut checkpoint_data = writer.checkpoint_data(&engine)?;
    /// let output_schema = checkpoint_data.output_schema().clone();
    /// while let Some(batch) = checkpoint_data.next() {
    ///     let data = batch?.apply_selection_vector()?;
    ///     parquet_writer.write(&data, &output_schema).await?;
    /// }
    /// writer.finalize(&engine, &metadata, checkpoint_data)?;
    /// ```
    pub fn checkpoint_data(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<TransformingCheckpointIterator> {
        let config = StatsTransformConfig::from_table_properties(self.snapshot.table_properties());

        // Get stats schema from table configuration.
        // This already excludes partition columns and applies column mapping.
        let stats_schema = self
            .snapshot
            .table_configuration()
            .expected_stats_schema()?;

        // Read schema includes stats_parsed so COALESCE expressions can operate on it.
        // For commits, stats_parsed will be read as nulls (column doesn't exist in source).
        let read_schema =
            build_checkpoint_read_schema_with_stats(&CHECKPOINT_ACTIONS_SCHEMA, &stats_schema);

        let is_v2_checkpoints_supported = self
            .snapshot
            .table_configuration()
            .is_feature_supported(&TableFeature::V2Checkpoint);

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

        // Build output schema based on stats config (determines which fields are included)
        let output_schema =
            build_checkpoint_output_schema(&config, &CHECKPOINT_ACTIONS_SCHEMA, &stats_schema);

        // Build transform expression and create expression evaluator
        let transform_expr = build_stats_transform(&config, stats_schema);
        let evaluator = engine.evaluation_handler().new_expression_evaluator(
            read_schema,
            transform_expr,
            output_schema.clone().into(),
        )?;

        // Create action reconciliation iterator (without checkpoint metadata)
        let inner = ActionReconciliationIterator::new(Box::new(checkpoint_data));

        // Handle V2 checkpoint metadata separately - it has a different schema
        // and shouldn't go through the stats transform
        let checkpoint_metadata = if is_v2_checkpoints_supported {
            let batch = self.create_checkpoint_metadata_batch(engine)?;
            Some(batch.filtered_data)
        } else {
            None
        };

        Ok(TransformingCheckpointIterator::new(
            inner,
            Some(evaluator),
            checkpoint_metadata,
            output_schema,
        ))
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
    /// - `checkpoint_data`: The exhausted checkpoint data iterator
    ///
    /// # Returns: `Ok` if the checkpoint was successfully finalized
    // Internally, this method:
    // 1. Validates that the checkpoint data iterator is fully exhausted
    // 2. Creates the `_last_checkpoint` data with `create_last_checkpoint_data`
    // 3. Writes the `_last_checkpoint` data to the `_last_checkpoint` file in the delta log
    pub fn finalize(
        self,
        engine: &dyn Engine,
        metadata: &FileMeta,
        checkpoint_data: TransformingCheckpointIterator,
    ) -> DeltaResult<()> {
        // Ensure the checkpoint data iterator is fully exhausted
        if !checkpoint_data.is_exhausted() {
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
            checkpoint_data.actions_count(),
            checkpoint_data.add_actions_count(),
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
    /// The function creates a single-row [`EngineData`] batch containing only the
    /// version field of the [`CheckpointMetadata`] action. Future implementations will
    /// include the additional metadata field `tags` when map support is added.
    ///
    /// # Returns:
    /// A [`ActionReconciliationBatch`] batch including the single-row [`EngineData`] batch along with
    /// an accompanying selection vector with a single `true` value, indicating the action in
    /// batch should be included in the checkpoint.
    fn create_checkpoint_metadata_batch(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<ActionReconciliationBatch> {
        let checkpoint_metadata_batch = engine.evaluation_handler().create_one(
            CHECKPOINT_METADATA_ACTION_SCHEMA.clone(),
            &[Scalar::from(self.version)],
        )?;

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
