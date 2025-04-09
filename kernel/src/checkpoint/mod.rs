//! # Delta Kernel Checkpoint API
//!
//! This module implements the API for writing checkpoints in delta tables.
//! Checkpoints provide a compact summary of the table state, enabling faster recovery by
//! avoiding full log replay. This API supports two checkpoint types:
//!
//! 1. **Single-file Classic-named V1 Checkpoint** – for legacy tables that do not support the
//!    `v2Checkpoints` reader/writer feature. These checkpoints follow the V1 specification and do not
//!    include a CheckpointMetadata action.
//! 2. **Single-file Classic-named V2 Checkpoint** – for tables supporting the `v2Checkpoints` feature.
//!    These checkpoints follow the V2 specification and include a CheckpointMetadata action, while
//!    maintaining backwards compatibility by using classic naming that legacy readers can recognize.
//!
//! For more information on the V1/V2 specifications, see the following protocol section:
//! <https://github.com/delta-io/delta/blob/master/PROTOCOL.md#checkpoint-specs>
//!
//! ### [`CheckpointWriter`]
//! Handles the actual checkpoint data generation and writing process. It is created via the
//! [`Table::checkpoint()`] method and provides the following APIs:
//! - `new(snapshot: Snapshot) -> Self` - Creates a new writer for the given table snapshot
//! - `checkpoint_data(engine: &dyn Engine) -> DeltaResult<SingleFileCheckpointData>` -
//!   Returns the checkpoint data and path information
//! - `finalize(engine: &dyn Engine, metadata: &dyn EngineData) -> DeltaResult<()>` -
//!   Writes the _last_checkpoint file after the checkpoint data has been written
//!
//! ## Checkpoint Type Selection
//!
//! The checkpoint type is determined by whether the table supports the `v2Checkpoints` reader/writer feature:
//!
//! ```text
//! +------------------+-------------------------------+
//! | Table Feature    | Resulting Checkpoint Type     |
//! +==================+===============================+
//! | No v2Checkpoints | Single-file Classic-named V1  |
//! +------------------+-------------------------------+
//! | v2Checkpoints    | Single-file Classic-named V2  |
//! +------------------+-------------------------------+
//! ```
//!
//! Notes:
//! - Single-file UUID-named V2 checkpoints (using `n.checkpoint.u.{json/parquet}` naming) are to be
//!   implemented in the future. The current implementation only supports classic-named V2 checkpoints.
//! - Multi-file V2 checkpoints are not supported yet. The API is designed to be extensible for future
//!   multi-file support, but the current implementation only supports single-file checkpoints.
//! - Multi-file V1 checkpoints are DEPRECATED and UNSAFE.
//!
//! ## Example: Writing a classic-named V1/V2 checkpoint (depending on `v2Checkpoints` feature support)
//!
//! TODO(seb): unignore example
//! ```ignore
//! let path = "./tests/data/app-txn-no-checkpoint";
//! let engine = Arc::new(SyncEngine::new());
//! let table = Table::try_from_uri(path)?;
//!
//! // Create a checkpoint writer for the table at a specific version
//! let mut writer = table.checkpoint(&engine, Some(2))?;
//!
//! // Retrieve checkpoint data
//! let checkpoint_data = writer.checkpoint_data()?;
//!
//! /* Write checkpoint data to file and collect metadata about the write */
//! /* The implementation of the write is storage-specific and not shown */
//! /* IMPORTANT: All data must be written before finalizing the checkpoint */
//!
//! // Finalize the checkpoint by writing the _last_checkpoint file
//! writer.finalize(&engine, &checkpoint_metadata)?;
//! ```
//!
//! This module, along with its submodule `checkpoint/log_replay.rs`, provides the full
//! API and implementation for generating checkpoints. See `checkpoint/log_replay.rs` for details
//! on how log replay is used to filter and deduplicate actions for checkpoint creation.
use crate::actions::{
    schemas::GetStructField, Add, Metadata, Protocol, Remove, SetTransaction, Sidecar, ADD_NAME,
    METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME, SET_TRANSACTION_NAME, SIDECAR_NAME,
};
use crate::expressions::Scalar;
use crate::path::ParsedLogPath;
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::snapshot::Snapshot;
#[cfg(doc)]
use crate::{actions::CheckpointMetadata, table::Table};
use crate::{DeltaResult, Engine, EngineData, Error, EvaluationHandlerExtension};
use log_replay::{checkpoint_actions_iter, CheckpointData};
use std::sync::atomic::AtomicI64;
use std::{
    sync::{Arc, LazyLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use url::Url;

mod log_replay;
#[cfg(test)]
mod tests;

/// Schema for extracting relevant actions from log files for checkpoint creation
static CHECKPOINT_ACTIONS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    StructType::new([
        Option::<Add>::get_struct_field(ADD_NAME),
        Option::<Remove>::get_struct_field(REMOVE_NAME),
        Option::<Metadata>::get_struct_field(METADATA_NAME),
        Option::<Protocol>::get_struct_field(PROTOCOL_NAME),
        Option::<SetTransaction>::get_struct_field(SET_TRANSACTION_NAME),
        Option::<Sidecar>::get_struct_field(SIDECAR_NAME),
    ])
    .into()
});

/// Returns the schema for reading Delta log actions for checkpoint creation
fn get_checkpoint_actions_schema() -> &'static SchemaRef {
    &CHECKPOINT_ACTIONS_SCHEMA
}

/// Represents a single-file checkpoint, including the data to write and the target path.
///
/// TODO(seb): Rename to `CheckpointData` once `FilteredEngineData` is introduced.
pub struct SingleFileCheckpointData {
    /// The URL where the checkpoint file should be written.
    pub path: Url,

    /// An iterator over the checkpoint data to be written to the file.
    pub data: Box<dyn Iterator<Item = DeltaResult<CheckpointData>>>,
}

/// Manages the checkpoint writing process for Delta tables
///
/// The [`CheckpointWriter`] orchestrates creating checkpoint data and finalizing
/// the checkpoint file. It tracks statistics about included actions and
/// ensures checkpoint data is consumed only once.
///
/// # Usage
/// 1. Create via [`Table::checkpoint()`]
/// 2. Call [`CheckpointWriter::checkpoint_data()`] to obtain [`SingleFileCheckpointData`],
///    containing the checkpoint path and data iterator
/// 3. Write the checkpoint data to storage (implementation-specific)
/// 4. Call [`CheckpointWriter::finalize()`] to create the _last_checkpoint file
///
/// # Internal Process
/// 1. Reads relevant actions from the log segment using the checkpoint read schema
/// 2. Applies selection and deduplication logic with the `CheckpointLogReplayProcessor`
/// 3. Tracks counts of included actions for to be written to the _last_checkpoint file
/// 5. Chains the [`CheckpointMetadata`] action to the actions iterator (for V2 checkpoints)
pub struct CheckpointWriter {
    /// Reference to the snapshot of the table being checkpointed
    pub(crate) snapshot: Arc<Snapshot>,
    /// Note: Arc<AtomicI64>> provides shared mutability for our counters, allowing the
    /// returned actions iterator from `.checkpoint_data()` to update the counters,
    /// and the `finalize()` method to read them...
    /// Counter for total actions included in the checkpoint
    pub(crate) total_actions_counter: Arc<AtomicI64>,
    /// Counter for Add actions included in the checkpoint
    pub(crate) total_add_actions_counter: Arc<AtomicI64>,
    /// Flag to track if checkpoint data has been consumed
    pub(crate) data_consumed: bool,
}

impl CheckpointWriter {
    /// Creates a new CheckpointWriter with the provided checkpoint data and counters
    pub(crate) fn new(snapshot: Arc<Snapshot>) -> Self {
        Self {
            snapshot,
            total_actions_counter: Arc::new(AtomicI64::new(0)),
            total_add_actions_counter: Arc::new(AtomicI64::new(0)),
            data_consumed: false,
        }
    }

    /// Retrieves the checkpoint data and path information
    ///
    /// This method is the core of the checkpoint generation process. It:
    ///
    /// 1. Ensures checkpoint data is consumed only once via `data_consumed` flag
    /// 2. Reads actions from the log segment using the checkpoint read schema
    /// 3. Filters and deduplicates actions for the checkpoint
    /// 4. Chains the checkpoint metadata action if writing a V2 spec checkpoint
    ///    (i.e., if `v2Checkpoints` feature is supported by table)
    /// 5. Generates the appropriate checkpoint path
    ///
    /// The returned data should be written to persistent storage by the caller
    /// before calling `finalize()` otherwise data loss may occur.
    ///
    /// # Returns
    /// A [`SingleFileCheckpointData`] containing the checkpoint path and action iterator
    pub fn checkpoint_data(
        &mut self,
        engine: &dyn Engine,
    ) -> DeltaResult<SingleFileCheckpointData> {
        if self.data_consumed {
            return Err(Error::generic("Checkpoint data has already been consumed"));
        }

        let is_v2_checkpoints_supported = self
            .snapshot
            .table_configuration()
            .is_v2_checkpoint_supported();

        let read_schema = get_checkpoint_actions_schema();
        let actions = self.snapshot.log_segment().read_actions(
            engine,
            read_schema.clone(),
            read_schema.clone(),
            None,
        );

        // Create iterator over actions for checkpoint data
        let checkpoint_data = checkpoint_actions_iter(
            actions?,
            self.total_actions_counter.clone(),
            self.total_add_actions_counter.clone(),
            self.deleted_file_retention_timestamp()?,
        );

        // Chain the checkpoint metadata action if using V2 checkpoints
        let chained = checkpoint_data.chain(self.create_checkpoint_metadata_batch(
            self.snapshot.version() as i64,
            engine,
            is_v2_checkpoints_supported,
        )?);

        let checkpoint_path = ParsedLogPath::new_classic_parquet_checkpoint(
            self.snapshot.table_root(),
            self.snapshot.version(),
        )?;

        self.data_consumed = true;

        Ok(SingleFileCheckpointData {
            path: checkpoint_path.location,
            data: Box::new(chained),
        })
    }

    /// Finalizes the checkpoint writing process by creating the _last_checkpoint file
    ///
    /// The `LastCheckpointInfo` (`_last_checkpoint`) file is a metadata file that contains
    /// information about the last checkpoint created for the table. It is used as a hint
    /// for the engine to quickly locate the last checkpoint and avoid full log replay when
    /// reading the table.
    ///
    /// # Workflow
    /// 0. IMPORTANT: This method must only be called AFTER successfully writing
    ///    all checkpoint data to storage. Failure to do so may result in
    ///    data loss.
    /// 1. Extracts size information from the provided metadata
    /// 2. Combines with additional metadata collected during checkpoint creation
    /// 3. Writes the _last_checkpoint file to the log
    ///
    /// # Parameters
    /// - `engine`: The engine used for writing the _last_checkpoint file
    /// - `metadata`: A single-row [`EngineData`] batch containing:
    ///   - `size_in_bytes` (i64): The size of the written checkpoint file
    #[allow(unused)]
    // TODO(seb): Implement finalize & then make pub. Pub(crate) for docs
    fn finalize(self, _engine: &dyn Engine, _metadata: &dyn EngineData) -> DeltaResult<()> {
        todo!("Implement finalize");
    }

    /// Creates the checkpoint metadata action for V2 checkpoints.
    ///
    /// For V2 checkpoints, this function generates a special [`CheckpointMetadata`] action
    /// that must be included in the V2 spec checkpoint file. This action contains metadata
    /// about the checkpoint, particularly its version. For V1 checkpoints, this function
    /// returns `None`, as the V1 schema does not include this action type.
    ///
    /// # Implementation Details
    ///
    /// The function creates a single-row [`EngineData`] batch containing only the
    /// version field of the `CheckpointMetadata` action. Future implementations will
    /// include additional metadata fields such as tags when map support is added.
    ///
    /// The resulting [`CheckpointData`] includes a selection vector with a single `true`
    /// value, indicating this action should always be included in the checkpoint.
    fn create_checkpoint_metadata_batch(
        &self,
        version: i64,
        engine: &dyn Engine,
        is_v2_checkpoint: bool,
    ) -> DeltaResult<Option<DeltaResult<CheckpointData>>> {
        if !is_v2_checkpoint {
            return Ok(None);
        }
        let values: &[Scalar] = &[version.into()];
        // Create the nested schema structure for `CheckpointMetadata`
        // Note: We cannot use `CheckpointMetadata::to_schema()` as it would include
        // the 'tags' field which we're not supporting yet due to the lack of map support.
        let schema = Arc::new(StructType::new([StructField::not_null(
            "checkpointMetadata",
            DataType::struct_type([StructField::not_null("version", DataType::LONG)]),
        )]));

        let checkpoint_metadata_batch = engine.evaluation_handler().create_one(schema, values)?;

        let result = CheckpointData {
            data: checkpoint_metadata_batch,
            selection_vector: vec![true], // Always include this action
        };

        // Ordering does not matter as there are no other threads modifying this counter
        // at this time (since we have not yet returned the iterator which performs the action counting)
        self.total_actions_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        Ok(Some(Ok(result)))
    }

    /// Calculates the cutoff timestamp for deleted file cleanup.
    ///
    /// This function determines the minimum timestamp before which deleted files
    /// will be permanently removed during VACUUM operations, based on the table's
    /// deleted_file_retention_duration property.
    ///
    /// Returns the cutoff timestamp in milliseconds since epoch, matching
    /// the remove action's deletion_timestamp format for comparison.
    ///
    /// The default retention period is 7 days, matching delta-spark's behavior.
    fn deleted_file_retention_timestamp(&self) -> DeltaResult<i64> {
        let retention_duration = self
            .snapshot
            .table_properties()
            .deleted_file_retention_duration;

        deleted_file_retention_timestamp_with_time(
            retention_duration,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| Error::generic(format!("Failed to calculate system time: {}", e)))?,
        )
    }
}

/// Internal implementation with injectable time parameter for testing
fn deleted_file_retention_timestamp_with_time(
    retention_duration: Option<Duration>,
    now_duration: Duration,
) -> DeltaResult<i64> {
    // Use provided retention duration or default (7 days)
    let retention_duration =
        retention_duration.unwrap_or_else(|| Duration::from_secs(60 * 60 * 24 * 7));

    // Convert to milliseconds for remove action deletion_timestamp comparison
    let now_ms: i64 = now_duration
        .as_millis()
        .try_into()
        .map_err(|_| Error::generic("Current timestamp exceeds i64 millisecond range"))?;

    let retention_ms: i64 = retention_duration
        .as_millis()
        .try_into()
        .map_err(|_| Error::generic("Retention duration exceeds i64 millisecond range"))?;

    // Simple subtraction - will produce negative values if retention > now
    Ok(now_ms - retention_ms)
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::engine::{arrow_data::ArrowEngineData, sync::SyncEngine};
    use crate::Table;
    use arrow_53::{array::RecordBatch, datatypes::Field};
    use delta_kernel::arrow::array::create_array;
    use std::path::PathBuf;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use crate::arrow::array::{ArrayRef, StructArray};
    use crate::arrow::datatypes::{DataType, Schema};

    #[test]
    fn test_deleted_file_retention_timestamp() -> DeltaResult<()> {
        let now = Duration::from_secs(1000).as_millis() as i64;

        // Test cases
        let test_cases = [
            // Default case (7 days)
            (None, now - (7 * 24 * 60 * 60 * 1000)),
            // Zero retention
            (Some(Duration::from_secs(0)), now),
            // Custom retention (2000 seconds)
            // This results in a negative timestamp which is valid - as it just means that
            // the retention window extends to before UNIX epoch.
            (Some(Duration::from_secs(2000)), now - (2000 * 1000)),
        ];

        for (retention, expected) in test_cases {
            let result =
                deleted_file_retention_timestamp_with_time(retention, Duration::from_secs(1000))?;
            assert_eq!(result, expected);
        }

        Ok(())
    }

    fn create_test_snapshot(engine: &dyn Engine) -> DeltaResult<Arc<Snapshot>> {
        let path = std::fs::canonicalize(PathBuf::from("./tests/data/app-txn-no-checkpoint/"));
        let url = url::Url::from_directory_path(path.unwrap()).unwrap();
        let table = Table::new(url);
        Ok(Arc::new(table.snapshot(engine, None)?))
    }

    #[test]
    fn test_create_checkpoint_metadata_batch_when_v2_checkpoints_is_supported() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let version = 10;
        let writer = CheckpointWriter::new(create_test_snapshot(&engine)?);

        // Test with is_v2_checkpoint = true
        let result = writer.create_checkpoint_metadata_batch(version, &engine, true)?;
        assert!(result.is_some());
        let checkpoint_data = result.unwrap()?;

        // Check selection vector has one true value
        assert_eq!(checkpoint_data.selection_vector, vec![true]);

        // Verify the underlying EngineData contains the expected CheckpointMetadata action
        let record_batch = checkpoint_data
            .data
            .any_ref()
            .downcast_ref::<ArrowEngineData>()
            .unwrap()
            .record_batch();

        // Build the expected RecordBatch
        // Note: The schema is a struct with a single field "checkpointMetadata" of type struct
        // containing a single field "version" of type long
        let expected_schema = Arc::new(Schema::new(vec![Field::new(
            "checkpointMetadata",
            DataType::Struct(vec![Field::new("version", DataType::Int64, false)].into()),
            false,
        )]));
        let expected = RecordBatch::try_new(
            expected_schema,
            vec![Arc::new(StructArray::from(vec![(
                Arc::new(Field::new("version", DataType::Int64, false)),
                create_array!(Int64, [version]) as ArrayRef,
            )]))],
        )
        .unwrap();

        assert_eq!(*record_batch, expected);

        // Verify counter was incremented
        assert_eq!(writer.total_actions_counter.load(Ordering::Relaxed), 1);

        Ok(())
    }

    #[test]
    fn test_create_checkpoint_metadata_batch_when_v2_checkpoints_not_supported() -> DeltaResult<()>
    {
        let engine = SyncEngine::new();
        let writer = CheckpointWriter::new(create_test_snapshot(&engine)?);

        // Test with is_v2_checkpoint = false
        let result = writer.create_checkpoint_metadata_batch(10, &engine, false)?;

        // No checkpoint metadata action should be created for V1 checkpoints
        assert!(result.is_none());

        // Verify counter was not incremented
        assert_eq!(writer.total_actions_counter.load(Ordering::Relaxed), 0);

        Ok(())
    }
}
