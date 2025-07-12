//! This module implements the API for writing single-file checkpoints.
//!
//! The entry-points for this API are:
//! 1. [`Snapshot::checkpoint`]
//! 2. [`Table::checkpoint`]
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
//! - [`CheckpointDataIterator`] - Iterator over the checkpoint data to be written
//!
//! ## Usage
//!
//! The following steps outline the process of creating a checkpoint:
//!
//! 1. Create a [`CheckpointWriter`] using [`Snapshot::checkpoint`] or [`Table::checkpoint`]
//! 2. Get the checkpoint path from [`CheckpointWriter::checkpoint_path`]
//! 2. Get the checkpoint data from [`CheckpointWriter::checkpoint_data`]
//! 3. Write the data to the path in object storage (engine-specific)
//! 4. Collect metadata ([`FileMeta`]) from the write operation
//! 5. Pass the metadata and exhausted data iterator to [`CheckpointWriter::finalize`]
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use delta_kernel::checkpoint::CheckpointDataIterator;
//! # use delta_kernel::checkpoint::CheckpointWriter;
//! # use delta_kernel::Engine;
//! # use delta_kernel::table::Table;
//! # use delta_kernel::DeltaResult;
//! # use delta_kernel::Error;
//! # use delta_kernel::FileMeta;
//! # use delta_kernel::snapshot::Snapshot;
//! # use url::Url;
//! fn write_checkpoint_file(path: Url, data: &CheckpointDataIterator) -> DeltaResult<FileMeta> {
//!     todo!() /* engine-specific logic to write data to object storage*/
//! }
//!
//! let engine: &dyn Engine = todo!(); /* create engine instance */
//!
//! // Create a table instance for the table you want to checkpoint
//! let table = Table::try_from_uri("./tests/data/app-txn-no-checkpoint")?;
//!
//! // Create a checkpoint writer from a version of the table (e.g., version 1)
//! // Alternatively, if you have a snapshot, you can use `Snapshot::checkpoint()`
//! let mut writer = table.checkpoint(engine, Some(1))?;
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
//! [`LastCheckpointHint`]: crate::snapshot::LastCheckpointHint
//! [`Table::checkpoint`]: crate::table::Table::checkpoint
// Future extensions:
// - TODO(#837): Multi-file V2 checkpoints are not supported yet. The API is designed to be extensible for future
//   multi-file support, but the current implementation only supports single-file checkpoints.
use std::sync::{Arc, LazyLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::actions::{
    Add, Metadata, Protocol, Remove, SetTransaction, Sidecar, ADD_NAME, CHECKPOINT_METADATA_NAME,
    METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME, SET_TRANSACTION_NAME, SIDECAR_NAME,
};
use crate::engine_data::FilteredEngineData;
use crate::expressions::Scalar;
use crate::log_replay::LogReplayProcessor;
use crate::path::ParsedLogPath;
use crate::schema::{DataType, SchemaRef, StructField, StructType, ToSchema as _};
use crate::snapshot::{Snapshot, LAST_CHECKPOINT_FILE_NAME};
use crate::utils::calculate_transaction_expiration_timestamp;
use crate::{DeltaResult, Engine, EngineData, Error, EvaluationHandlerExtension, FileMeta};
use log_replay::{CheckpointBatch, CheckpointLogReplayProcessor};

use url::Url;

mod log_replay;
#[cfg(test)]
mod tests;

const SECONDS_PER_MINUTE: u64 = 60;
const MINUTES_PER_HOUR: u64 = 60;
const HOURS_PER_DAY: u64 = 24;
/// The default retention period for deleted files in seconds.
/// This is set to 7 days, which is the default in delta-spark.
const DEFAULT_RETENTION_SECS: u64 = 7 * HOURS_PER_DAY * MINUTES_PER_HOUR * SECONDS_PER_MINUTE;

/// Schema of the `_last_checkpoint` file
/// We cannot use `LastCheckpointInfo::to_schema()` as it would include the 'checkpoint_schema'
/// field, which is only known at runtime.
static LAST_CHECKPOINT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    StructType::new([
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
    Arc::new(StructType::new([
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
    Arc::new(StructType::new([StructField::nullable(
        CHECKPOINT_METADATA_NAME,
        DataType::struct_type([StructField::not_null("version", DataType::LONG)]),
    )]))
});

/// An iterator over the checkpoint data to be written to the file.
///
/// This iterator yields filtered checkpoint data batches ([`FilteredEngineData`]) and
/// tracks action statistics required for finalizing the checkpoint.
///
/// # Warning
/// The [`CheckpointDataIterator`] must be fully consumed to ensure proper collection of statistics for
/// the checkpoint. Additionally, all yielded data must be written to the specified path before calling
/// [`CheckpointWriter::finalize`]. Failing to do so may result in data loss or corruption.
pub struct CheckpointDataIterator {
    /// The nested iterator that yields checkpoint batches with action counts
    checkpoint_batch_iterator: Box<dyn Iterator<Item = DeltaResult<CheckpointBatch>> + Send>,
    /// Running total of actions included in the checkpoint
    actions_count: i64,
    /// Running total of add actions included in the checkpoint
    add_actions_count: i64,
}

impl Iterator for CheckpointDataIterator {
    type Item = DeltaResult<FilteredEngineData>;

    /// Advances the iterator and returns the next value.
    ///
    /// This implementation transforms the `CheckpointBatch` items from the nested iterator into
    /// [`FilteredEngineData`] items for the engine to write, while accumulating action counts from
    /// each batch. The [`CheckpointDataIterator`] is passed back to the kernel on call to
    /// [`CheckpointWriter::finalize`] for counts to be read and written to the `_last_checkpoint` file
    fn next(&mut self) -> Option<Self::Item> {
        Some(self.checkpoint_batch_iterator.next()?.map(|batch| {
            self.actions_count += batch.actions_count;
            self.add_actions_count += batch.add_actions_count;
            batch.filtered_data
        }))
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
pub struct CheckpointWriter {
    /// Reference to the snapshot (i.e. version) of the table being checkpointed
    pub(crate) snapshot: Arc<Snapshot>,

    /// The version of the snapshot being checkpointed.
    /// Note: Although the version is stored as a u64 in the snapshot, it is stored as an i64
    /// field here to avoid multiple type conversions.
    version: i64,
}

impl CheckpointWriter {
    /// Creates a new [`CheckpointWriter`] for the given snapshot.
    pub(crate) fn try_new(snapshot: Arc<Snapshot>) -> DeltaResult<Self> {
        let version = i64::try_from(snapshot.version()).map_err(|e| {
            Error::CheckpointWrite(format!(
                "Failed to convert checkpoint version from u64 {} to i64: {}",
                snapshot.version(),
                e
            ))
        })?;

        Ok(Self { snapshot, version })
    }

    fn get_transaction_expiration_timestamp(&self) -> DeltaResult<Option<i64>> {
        calculate_transaction_expiration_timestamp(self.snapshot.table_properties())
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
    /// This method reads the actions from the log segment and processes them
    /// to create the checkpoint data.
    ///
    /// # Parameters
    /// - `engine`: Implementation of [`Engine`] APIs.
    ///
    /// # Returns: [`CheckpointDataIterator`] containing the checkpoint data
    // This method is the core of the checkpoint generation process. It:
    // 1. Determines whether to write a V1 or V2 checkpoint based on the table's
    //    `v2Checkpoints` feature support
    // 2. Reads actions from the log segment using the checkpoint read schema
    // 3. Filters and deduplicates actions for the checkpoint
    // 4. Chains the checkpoint metadata action if writing a V2 spec checkpoint
    //    (i.e., if `v2Checkpoints` feature is supported by table)
    // 5. Generates the appropriate checkpoint path
    pub fn checkpoint_data(&self, engine: &dyn Engine) -> DeltaResult<CheckpointDataIterator> {
        let is_v2_checkpoints_supported = self
            .snapshot
            .table_configuration()
            .is_v2_checkpoint_write_supported();

        let actions = self.snapshot.log_segment().read_actions(
            engine,
            CHECKPOINT_ACTIONS_SCHEMA.clone(),
            CHECKPOINT_ACTIONS_SCHEMA.clone(),
            None,
        )?;

        // Create iterator over actions for checkpoint data
        let checkpoint_data = CheckpointLogReplayProcessor::new(
            self.deleted_file_retention_timestamp()?,
            self.get_transaction_expiration_timestamp()?,
        )
        .process_actions_iter(actions);

        let checkpoint_metadata =
            is_v2_checkpoints_supported.then(|| self.create_checkpoint_metadata_batch(engine));

        // Wrap the iterator in a CheckpointDataIterator to track action counts
        Ok(CheckpointDataIterator {
            checkpoint_batch_iterator: Box::new(checkpoint_data.chain(checkpoint_metadata)),
            actions_count: 0,
            add_actions_count: 0,
        })
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
        mut checkpoint_data: CheckpointDataIterator,
    ) -> DeltaResult<()> {
        // Ensure the checkpoint data iterator is fully exhausted
        if checkpoint_data.checkpoint_batch_iterator.next().is_some() {
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
            checkpoint_data.actions_count,
            checkpoint_data.add_actions_count,
            size_in_bytes,
        );

        let last_checkpoint_path = self
            .snapshot
            .log_segment()
            .log_root
            .join(LAST_CHECKPOINT_FILE_NAME)?;

        // Write the `_last_checkpoint` file to `table/_delta_log/_last_checkpoint`
        engine.json_handler().write_json_file(
            &last_checkpoint_path,
            Box::new(std::iter::once(data)),
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
    /// A [`CheckpointBatch`] batch including the single-row [`EngineData`] batch along with
    /// an accompanying selection vector with a single `true` value, indicating the action in
    /// batch should be included in the checkpoint.
    fn create_checkpoint_metadata_batch(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<CheckpointBatch> {
        let checkpoint_metadata_batch = engine.evaluation_handler().create_one(
            CHECKPOINT_METADATA_ACTION_SCHEMA.clone(),
            &[Scalar::from(self.version)],
        )?;

        let filtered_data = FilteredEngineData {
            data: checkpoint_metadata_batch,
            selection_vector: vec![true], // Include the action in the checkpoint
        };

        Ok(CheckpointBatch {
            filtered_data,
            actions_count: 1,
            add_actions_count: 0,
        })
    }

    /// This function determines the minimum timestamp before which deleted files
    /// are eligible for permanent removal during VACUUM operations. It is used
    /// during checkpointing to decide whether to include `remove` actions.
    ///
    /// If a deleted file's timestamp is older than this threshold (based on the
    /// table's `deleted_file_retention_duration`), the corresponding `remove` action
    /// is included in the checkpoint, allowing VACUUM operations to later identify
    /// and clean up those files.
    ///
    /// # Returns:
    /// The cutoff timestamp in milliseconds since epoch, matching the remove action's
    /// `deletion_timestamp` field format for comparison.
    ///
    /// # Note: The default retention period is 7 days, matching delta-spark's behavior.
    fn deleted_file_retention_timestamp(&self) -> DeltaResult<i64> {
        let retention_duration = self
            .snapshot
            .table_properties()
            .deleted_file_retention_duration;

        deleted_file_retention_timestamp_with_time(
            retention_duration,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| Error::generic(format!("Failed to calculate system time: {e}")))?,
        )
    }
}

/// Calculates the timestamp threshold for deleted file retention based on the provided duration.
/// This is factored out to allow testing with an injectable time and duration parameter.
///
/// # Parameters
/// - `retention_duration`: The duration to retain deleted files. The table property
///   `deleted_file_retention_duration` is passed here. If `None`, defaults to 7 days.
/// - `now_duration`: The current time as a [`Duration`]. This allows for testing with
///   a specific time instead of using `SystemTime::now()`.
///
/// # Returns: The timestamp in milliseconds since epoch
fn deleted_file_retention_timestamp_with_time(
    retention_duration: Option<Duration>,
    now_duration: Duration,
) -> DeltaResult<i64> {
    // Use provided retention duration or default (7 days)
    let retention_duration =
        retention_duration.unwrap_or_else(|| Duration::from_secs(DEFAULT_RETENTION_SECS));

    // Convert to milliseconds for remove action deletion_timestamp comparison
    let now_ms = i64::try_from(now_duration.as_millis())
        .map_err(|_| Error::checkpoint_write("Current timestamp exceeds i64 millisecond range"))?;

    let retention_ms = i64::try_from(retention_duration.as_millis())
        .map_err(|_| Error::checkpoint_write("Retention duration exceeds i64 millisecond range"))?;

    // Simple subtraction - will produce negative values if retention > now
    Ok(now_ms - retention_ms)
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
