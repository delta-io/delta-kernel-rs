//! This module implements the API for writing Delta checkpoints.
//!
//! The entry point for this API is [`Snapshot::create_checkpoint_writer`].
//!
//! ## Checkpoint Types and Selection Logic
//! This API supports two checkpoint types, selected based on table features:
//!
//! | Table Feature    | Resulting Checkpoint Type    | Description                                                                 |
//! |------------------|-------------------------------|-----------------------------------------------------------------------------|
//! | No v2Checkpoints | Single-file Classic-named V1 | Follows V1 specification without [`CheckpointMetadata`] action             |
//! | v2Checkpoints    | Classic-named V2             | Follows V2 specification with [`CheckpointMetadata`] action while maintaining backward compatibility via classic naming |
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
//! 3. Get the checkpoint data from [`CheckpointWriter::checkpoint_data`]
//! 4. Write the data to the path in object storage (engine-specific)
//! 5. Collect metadata ([`FileMeta`]) from the write operation
//! 6. Build a [`LastCheckpointHintStats`] from the exhausted iterator state
//! 7. Pass the [`LastCheckpointHintStats`] to [`CheckpointWriter::finalize`]
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
//! fn write_checkpoint_file(path: Url, data: ActionReconciliationIterator) -> DeltaResult<FileMeta> {
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
//! let writer = snapshot.create_checkpoint_writer()?;
//!
//! // Get the checkpoint path and data
//! let checkpoint_path = writer.checkpoint_path()?;
//! let checkpoint_data = writer.checkpoint_data(engine)?;
//!
//! // Get the iterator state before consuming the data
//! let state = checkpoint_data.state();
//!
//! // Write the checkpoint data to the object store and collect metadata
//! // The write function consumes the iterator, dropping its Arc reference to the state.
//! let metadata: FileMeta = write_checkpoint_file(checkpoint_path, checkpoint_data)?;
//! /* IMPORTANT: All data must be written before finalizing the checkpoint */
//!
//! // Build the [`LastCheckpointHintStats`] from the exhausted iterator state
//! let state = std::sync::Arc::into_inner(state)
//!     .ok_or(Error::internal_error("checkpoint state Arc still has other references"))?;
//! let last_checkpoint_stats =
//!     delta_kernel::checkpoint::LastCheckpointHintStats::from_reconciliation_state(
//!         state,
//!         metadata.size,
//!         0, /* num_sidecars */
//!     )?;
//!
//! // Finalize the checkpoint by passing the stats
//! writer.finalize(engine, &last_checkpoint_stats)?;
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
//! [`FileMeta`]: crate::FileMeta
//! [`LastCheckpointHint`]: crate::last_checkpoint_hint::LastCheckpointHint
//! [`Snapshot::create_checkpoint_writer`]: crate::Snapshot::create_checkpoint_writer
use std::sync::{Arc, LazyLock, Mutex, OnceLock};

use tracing::info;
use url::Url;

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
use crate::path::{self, ParsedLogPath};
use crate::schema::{DataType, SchemaRef, StructField, StructType, ToSchema as _};
use crate::snapshot::SnapshotRef;
use crate::table_features::TableFeature;
use crate::table_properties::TableProperties;
use crate::{
    DeltaResult, Engine, EngineData, Error, EvaluationHandlerExtension, FileMeta, Version,
};

mod checkpoint_transform;
mod sidecar;

use checkpoint_transform::{
    build_checkpoint_output_schema, build_checkpoint_read_schema, build_checkpoint_transform,
    StatsTransformConfig,
};
use sidecar::{create_sidecar_action_batches, SidecarSplitter, SingleSidecarDataIterator};
#[cfg(test)]
mod tests;

/// Information about a freshly-written checkpoint. Pass it to
/// [`CheckpointWriter::finalize`] to produce the `_last_checkpoint` hint file.
///
/// Unlike [`LastCheckpointHint`], which is used on the read path and has many optional
/// fields, this struct focuses on the write path and all fields are required. Note this
/// is a kernel requirement; the Delta protocol itself marks some of these fields as
/// optional in the `_last_checkpoint` hint. See the [Last Checkpoint File Schema] for
/// more details.
///
/// Construct via [`LastCheckpointHintStats::from_reconciliation_state`].
///
/// # Note
/// This is intended for sophisticated connectors that customize how checkpoints are
/// written. If you are not deeply familiar with the Delta protocol, you may use
/// [`Snapshot::checkpoint`](crate::snapshot::Snapshot::checkpoint), which handles
/// checkpoint writing end-to-end.
///
/// [Last Checkpoint File Schema]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#last-checkpoint-file-schema
#[derive(Debug)]
pub struct LastCheckpointHintStats {
    /// The total number of actions stored in the checkpoint (including actions created
    /// outside the reconciliation iterator, e.g. sidecar actions).
    num_actions: i64,
    /// The total size in bytes of the checkpoint. For V2 checkpoint with sidecars,
    /// this is the sum of the main checkpoint file size and all sidecar file sizes.
    size_in_bytes: i64,
    /// The number of Add-file actions in the checkpoint.
    num_of_add_files: i64,
}

impl LastCheckpointHintStats {
    /// Constructs a `LastCheckpointHintStats` from the fully-exhausted reconciliation state.
    ///
    /// # Parameters
    /// - `state`: fully-exhausted reconciliation iterator state. All data must have been written to
    ///   storage before calling this.
    /// - `size_in_bytes`: total byte size of the checkpoint. For V2 checkpoints with sidecars, the
    ///   caller should include the main checkpoint file size plus all sidecar file sizes.
    /// - `num_sidecars`: number of sidecar actions. Use `0` for V1 checkpoints or V2 checkpoints
    ///   without sidecars.
    ///
    /// # Errors
    /// - If the reconciliation iterator has not been fully exhausted.
    /// - If `size_in_bytes` exceeds `i64::MAX`.
    /// - If `num_sidecars` exceeds `i64::MAX`.
    /// - If `state.actions_count() + num_sidecars` overflows `i64`.
    pub fn from_reconciliation_state(
        state: ActionReconciliationIteratorState,
        size_in_bytes: u64,
        num_sidecars: u64,
    ) -> DeltaResult<Self> {
        if !state.is_exhausted() {
            return Err(Error::checkpoint_write(
                "Cannot build LastCheckpointHintStats: the reconciliation iterator must be fully \
                 consumed and all data written to storage before finalizing",
            ));
        }
        let size_in_bytes = i64::try_from(size_in_bytes).map_err(|e| {
            Error::checkpoint_write(format!("size_in_bytes {size_in_bytes} exceeds i64: {e}"))
        })?;
        let num_sidecars_i64 = i64::try_from(num_sidecars).map_err(|e| {
            Error::checkpoint_write(format!("num_sidecars {num_sidecars} exceeds i64: {e}"))
        })?;
        let num_actions = state
            .actions_count()
            .checked_add(num_sidecars_i64)
            .ok_or_else(|| {
                Error::checkpoint_write(format!(
                    "checkpoint action count overflowed i64: {} + {num_sidecars}",
                    state.actions_count()
                ))
            })?;
        Ok(Self {
            num_actions,
            size_in_bytes,
            num_of_add_files: state.add_actions_count(),
        })
    }
}

#[derive(Debug)]
pub(crate) struct WrittenCheckpointInfo {
    /// Metadata of the main checkpoint file.
    /// SIDECAR_TODO: Remove the #[allow(dead_code)] in next PR
    #[allow(dead_code)]
    pub(crate) file_meta: FileMeta,
    /// Stats for the `_last_checkpoint` hint.
    pub(crate) last_checkpoint_stats: LastCheckpointHintStats,
}

/// Schemas and configs needed for building the checkpoint read/output schemas.
struct CheckpointSchemaContext {
    stats_config: StatsTransformConfig,
    /// The checkpoint schema before table-specific fields like
    /// `stats_parsed` and `partitionValues_parsed` are injected.
    checkpoint_base_schema: SchemaRef,
    stats_schema: SchemaRef,
    partition_schema: Option<SchemaRef>,
    is_v2: bool,
}

/// Default value for [`V2CheckpointConfig::WithSidecar::file_actions_per_sidecar_hint`].
/// It's the suggested upper bound of file actions (`add` and `remove`) per sidecar file when
/// the caller does not provide an explicit hint.
pub const DEFAULT_FILE_ACTIONS_PER_SIDECAR_HINT: usize = 50_000;

/// Specifies the checkpoint format and behavior.
#[derive(Debug)]
pub enum CheckpointSpec {
    /// Write a checkpoint following the V1 spec, the original checkpoint format, without
    /// sidecar files or checkpoint metadata. See [V1 spec] for more details.
    ///
    /// [V1 spec]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#v1-spec
    V1,
    /// Write a checkpoint following the V2 spec, which allows putting file actions (`add`
    /// and `remove`) in sidecar files. Requires the `v2Checkpoint` reader/writer feature.
    /// See [V2 spec] and [`V2CheckpointConfig::WithSidecar`] for more details.
    ///
    /// [V2 spec]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#v2-spec
    V2(V2CheckpointConfig),
}

/// Configuration for V2 checkpoints.
///
/// Note: "File actions" here means `add` and `remove` actions. "Non-file actions" means
/// the rest (`protocol`, `metaData`, `txn`, etc.).
#[derive(Debug)]
pub enum V2CheckpointConfig {
    /// Write a V2 checkpoint without sidecar files.
    NoSidecar,
    /// Write a V2 checkpoint with file actions split into sidecar files. A main
    /// checkpoint file is written, with one `sidecar` action pointing to each sidecar file.
    ///
    /// # Benefits of Sidecars
    /// - **Read parallelism**: readers can fetch sidecars in parallel.
    /// - **Smaller main checkpoint**: callers that only need non-file actions (e.g. `protocol`,
    ///   `metaData`) can skip the sidecars entirely.
    ///
    /// # Note
    /// Sidecars add extra write cost (one parquet file per sidecar).
    ///
    /// [Sidecar Files]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#sidecar-files
    WithSidecar {
        /// Suggested number of file actions per sidecar file. When there are X file actions,
        /// the number of sidecars will roughly be `X / file_actions_per_sidecar_hint`.
        ///
        /// This is a hint, not a strict limit, because file actions are stored in `EngineData`
        /// batches that cannot be split. For example, if the hint is 99 but a single
        /// `EngineData` batch contains 100 file actions, all 100 will be written to one sidecar.
        ///
        /// When `None`, kernel uses [`DEFAULT_FILE_ACTIONS_PER_SIDECAR_HINT`] (50,000) as the
        /// default.
        file_actions_per_sidecar_hint: Option<usize>,
    },
}

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
/// The checkpoint data must be fully written to storage before calling
/// [`CheckpointWriter::finalize`]. Failing to do so may result in data loss or corruption.
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

    /// Cached checkpoint output schema.
    checkpoint_output_schema: OnceLock<SchemaRef>,
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
            checkpoint_output_schema: OnceLock::new(),
        })
    }
    /// Returns the cached output schema, initializing it with `f` on first call.
    ///
    /// `OnceLock::get_or_try_init` is unstable, so we use a custom implementation.
    /// (tracking issue: <https://github.com/rust-lang/rust/issues/109737>).
    fn get_or_init_output_schema(
        &self,
        f: impl FnOnce() -> DeltaResult<SchemaRef>,
    ) -> DeltaResult<SchemaRef> {
        if let Some(schema) = self.checkpoint_output_schema.get() {
            return Ok(schema.clone());
        }
        let schema = f()?;
        let _ = self.checkpoint_output_schema.set(schema);
        self.checkpoint_output_schema
            .get()
            .cloned()
            .ok_or_else(|| Error::internal_error("OnceLock should be initialized"))
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
    /// shared state for building a [`LastCheckpointHintStats`] after the iterator is exhausted.
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
    /// drop(checkpoint_data);
    /// let state = Arc::into_inner(state)
    ///     .ok_or(Error::internal_error("checkpoint state Arc still has other references"))?;
    /// let last_checkpoint_stats =
    ///     LastCheckpointHintStats::from_reconciliation_state(state, size_in_bytes, 0)?;
    /// writer.finalize(&engine, &last_checkpoint_stats)?;
    /// ```
    // Implementation overview:
    // 1. Determines whether to write a V1 or V2 checkpoint based on `v2Checkpoints` feature
    // 2. Builds a read schema with stats_parsed for COALESCE expressions
    // 3. Reads actions from the log segment and deduplicates via reconciliation
    // 4. Applies stats transforms (COALESCE/drop) to each reconciled batch
    // 5. Chains the checkpoint metadata action for V2 checkpoints
    pub fn checkpoint_data(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<ActionReconciliationIterator> {
        let schema_context = self.checkpoint_schema_context(engine)?;

        // The read schema and output schema differ because the transform needs access to
        // both stats formats as input, but may only write one format as output.
        //
        // read_schema: Always includes both `stats` and `stats_parsed` fields in the Add
        // action, so COALESCE expressions can read from either source. For commit files,
        // `stats_parsed` doesn't exist and is read as nulls. For partitioned tables,
        // `partitionValues_parsed` is also included.
        //
        // output_schema: Only includes the stats fields that the table config requests
        // (e.g., only `stats` if writeStatsAsJson=true and writeStatsAsStruct=false).
        let read_schema = build_checkpoint_read_schema(
            &schema_context.checkpoint_base_schema,
            &schema_context.stats_schema,
            schema_context.partition_schema.as_deref(),
        )?;

        // Read actions from log segment
        let actions = self
            .snapshot
            .log_segment()
            .read_actions(engine, read_schema.clone())?;

        // Process actions through reconciliation
        let checkpoint_data = ActionReconciliationProcessor::new(
            self.deleted_file_retention_timestamp()?,
            self.get_transaction_expiration_timestamp()?,
        )
        .process_actions_iter(actions);

        let output_schema = self.get_or_init_output_schema(|| {
            build_checkpoint_output_schema(
                &schema_context.stats_config,
                &schema_context.checkpoint_base_schema,
                &schema_context.stats_schema,
                schema_context.partition_schema.as_deref(),
            )
        })?;

        // Build transform expression and create expression evaluator.
        // The transform is applied to reconciled action batches only (not checkpoint metadata).
        let transform_expr = build_checkpoint_transform(
            &schema_context.stats_config,
            &schema_context.stats_schema,
            schema_context.partition_schema.as_ref(),
        );
        let evaluator = engine.evaluation_handler().new_expression_evaluator(
            read_schema,
            transform_expr,
            output_schema.clone().into(),
        )?;

        // Apply stats transform to each reconciled batch
        let transformed = checkpoint_data.map(move |batch_result| {
            let batch = batch_result?;
            let (data, sv) = batch.filtered_data.into_parts();
            let transformed = evaluator.evaluate(data.as_ref())?;
            Ok(ActionReconciliationBatch {
                filtered_data: FilteredEngineData::try_new(transformed, sv)?,
                actions_count: batch.actions_count,
                add_actions_count: batch.add_actions_count,
            })
        });

        // For V2 checkpoints, chain the checkpoint metadata batch after the transformed
        // action stream. The metadata batch is created with the output schema directly,
        // bypassing the stats transform (it has no add actions to transform).
        let checkpoint_metadata = schema_context
            .is_v2
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
    /// - `engine`: Implementation of [`Engine`] APIs.
    /// - `last_checkpoint_stats`: The [`LastCheckpointHintStats`] containing fields needed to write
    ///   the `_last_checkpoint` file.
    ///
    /// # Returns: `Ok` if the checkpoint was successfully finalized
    // Internally, this method:
    // 1. Creates the `_last_checkpoint` data with `create_last_checkpoint_data`
    // 2. Writes the `_last_checkpoint` data to the `_last_checkpoint` file in the delta log
    pub fn finalize(
        self,
        engine: &dyn Engine,
        last_checkpoint_stats: &LastCheckpointHintStats,
    ) -> DeltaResult<()> {
        // Skip writing `_last_checkpoint` if the existing hint already points to a newer
        // checkpoint, to avoid regressing the hint.
        let checkpoint_version = self.snapshot.version();
        if let Some(hint_version) = self.snapshot.log_segment().last_checkpoint_version() {
            if hint_version > checkpoint_version {
                info!(
                    hint_version,
                    checkpoint_version,
                    "Skipping _last_checkpoint write: existing hint is newer than checkpoint"
                );
                return Ok(());
            }
        }

        let data = create_last_checkpoint_data(
            engine,
            self.version,
            last_checkpoint_stats.num_actions,
            last_checkpoint_stats.num_of_add_files,
            last_checkpoint_stats.size_in_bytes,
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

    pub(crate) fn write_v2_checkpoint_with_sidecars(
        &self,
        engine: &dyn Engine,
        file_actions_per_sidecar_hint: usize,
    ) -> DeltaResult<WrittenCheckpointInfo> {
        let output_schema = self.get_or_init_output_schema(|| {
            let ctx = self.checkpoint_schema_context(engine)?;
            build_checkpoint_output_schema(
                &ctx.stats_config,
                &ctx.checkpoint_base_schema,
                &ctx.stats_schema,
                ctx.partition_schema.as_deref(),
            )
        })?;
        let data_iter = self.checkpoint_data(engine)?;
        let iter_state = data_iter.state();

        let splitter = SidecarSplitter::new_mut_shared(
            data_iter,
            engine.evaluation_handler().as_ref(),
            output_schema.clone(),
        )?;

        // Write sidecar files
        let mut sidecar_metas: Vec<(String, FileMeta)> = Vec::new();
        loop {
            if let Some(entry) = write_single_sidecar(
                engine,
                &splitter,
                file_actions_per_sidecar_hint,
                self.snapshot.table_root(),
                self.snapshot.version(),
            )? {
                sidecar_metas.push(entry);
            }
            let is_exhausted = splitter
                .lock()
                .map_err(|e| Error::internal_error(format!("sidecar splitter lock poisoned: {e}")))?
                .is_exhausted();
            if is_exhausted {
                break;
            }
        }

        // Collect non-file action batches(e.g. `protocol`, `metaData`, `txn`, etc.)
        let non_file_batches = Arc::into_inner(splitter)
            .ok_or_else(|| {
                Error::internal_error("sidecar splitter Arc should have no other references")
            })?
            .into_inner()
            .map_err(|e| Error::internal_error(format!("sidecar splitter lock poisoned: {e}")))?
            .into_non_file_batches();

        // Create sidecar action rows for the main checkpoint file. Each row populates only
        // the `sidecar` column, e.g. `sidecar: { path: "<sidecar_filename>.parquet", sizeInBytes:
        // <size>, modificationTime: <time>, tags: null }`, with all other action columns
        // left null.
        let sidecar_batches =
            create_sidecar_action_batches(engine, &output_schema, &sidecar_metas)?;

        // Write main checkpoint file: non-file actions + sidecar references
        let checkpoint_path = self.checkpoint_path()?;
        let main_data: Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send> =
            Box::new(non_file_batches.into_iter().chain(sidecar_batches).map(Ok));
        engine
            .parquet_handler()
            .write_parquet_file(checkpoint_path.clone(), main_data)?;

        // Size_in_bytes covers the main checkpoint file plus all sidecar files.
        let sidecar_sizes_sum = sidecar_metas
            .iter()
            .try_fold(0u64, |acc, (_, m)| acc.checked_add(m.size))
            .ok_or_else(|| Error::internal_error("sidecar sizes sum overflowed u64"))?;
        let sidecar_count = u64::try_from(sidecar_metas.len()).map_err(|e| {
            Error::internal_error(format!(
                "Failed to convert sidecar count from usize {} to u64: {e}",
                sidecar_metas.len()
            ))
        })?;
        build_written_checkpoint_info(
            engine,
            &checkpoint_path,
            iter_state,
            sidecar_sizes_sum,
            sidecar_count,
        )
    }

    /// Writes a checkpoint without sidecars, will automatically choose V1 or V2
    /// based on the table features.
    pub(crate) fn write_checkpoint_without_sidecars(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<WrittenCheckpointInfo> {
        let checkpoint_path = self.checkpoint_path()?;
        let data_iter = self.checkpoint_data(engine)?;
        let state = data_iter.state();
        let lazy_data = data_iter.map(|r| r.and_then(|f| f.apply_selection_vector()));
        engine
            .parquet_handler()
            .write_parquet_file(checkpoint_path.clone(), Box::new(lazy_data))?;

        build_written_checkpoint_info(
            engine,
            &checkpoint_path,
            state,
            0, /* sidecar_sizes_sum */
            0, /* sidecar_count */
        )
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

    /// Helper for computing the checkpoint schema context from the snapshot and engine.
    fn checkpoint_schema_context(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<CheckpointSchemaContext> {
        let tc = self.snapshot.table_configuration();
        let config = StatsTransformConfig::from_table_properties(self.snapshot.table_properties());

        // Select schema based on V2 checkpoint support
        let is_v2 = tc.is_feature_supported(&TableFeature::V2Checkpoint);
        let base_schema = if is_v2 {
            CHECKPOINT_ACTIONS_SCHEMA_V2.clone()
        } else {
            CHECKPOINT_ACTIONS_SCHEMA_V1.clone()
        };

        // Get clustering columns so they are always included in stats per the Delta protocol.
        let physical_clustering_columns = self.snapshot.get_physical_clustering_columns(engine)?;

        // Get stats schema from table configuration.
        // This already excludes partition columns and applies column mapping.
        let stats_schema = tc
            .build_expected_stats_schemas(physical_clustering_columns.as_deref(), None)?
            .physical;

        // Build partition schema for partitionValues_parsed (None for non-partitioned tables)
        let partition_schema = tc.build_partition_values_parsed_schema();
        Ok(CheckpointSchemaContext {
            stats_config: config,
            checkpoint_base_schema: base_schema,
            stats_schema,
            partition_schema,
            is_v2,
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

/// Writes one sidecar file. Returns `None` if the splitter yielded no rows for this sidecar.
fn write_single_sidecar(
    engine: &dyn Engine,
    splitter: &Arc<Mutex<SidecarSplitter>>,
    file_actions_per_sidecar_hint: usize,
    table_root: &Url,
    version: Version,
) -> DeltaResult<Option<(String, FileMeta)>> {
    let mut iter =
        SingleSidecarDataIterator::new(splitter.clone(), file_actions_per_sidecar_hint)?.peekable();
    if iter.peek().is_none() {
        return Ok(None);
    }
    let (filename, sidecar_url) = path::new_sidecar(table_root, version)?;
    engine
        .parquet_handler()
        .write_parquet_file(sidecar_url.clone(), Box::new(iter))?;
    let meta = engine.storage_handler().head(&sidecar_url)?;
    Ok(Some((filename, meta)))
}

fn build_written_checkpoint_info(
    engine: &dyn Engine,
    checkpoint_path: &Url,
    state: Arc<ActionReconciliationIteratorState>,
    sidecar_sizes_sum: u64,
    sidecar_count: u64,
) -> DeltaResult<WrittenCheckpointInfo> {
    let file_meta = engine.storage_handler().head(checkpoint_path)?;
    let total_size_in_bytes = file_meta
        .size
        .checked_add(sidecar_sizes_sum)
        .ok_or_else(|| Error::internal_error("checkpoint total size_in_bytes overflowed u64"))?;
    let state = Arc::into_inner(state).ok_or_else(|| {
        Error::internal_error("ActionReconciliationIteratorState Arc has other references")
    })?;
    let last_checkpoint_stats = LastCheckpointHintStats::from_reconciliation_state(
        state,
        total_size_in_bytes,
        sidecar_count,
    )?;
    Ok(WrittenCheckpointInfo {
        file_meta,
        last_checkpoint_stats,
    })
}
