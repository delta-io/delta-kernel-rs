//! # Delta Kernel Checkpoint API
//!
//! This module implements the API for writing checkpoints in delta tables.
//! Checkpoints provide a compact summary of the table state, enabling faster recovery by
//! avoiding full log replay. This API supports two checkpoint types:
//!
//! 1. **Single-file Classic-named V1 Checkpoint** – for legacy tables that do not support the
//!    `v2Checkpoints` reader/writer feature. These checkpoints follow the V1 specification and do not
//!    include a [`CheckpointMetadata`] action.
//! 2. **Single-file Classic-named V2 Checkpoint** – for tables supporting the `v2Checkpoints` feature.
//!    These checkpoints follow the V2 specification and include a [`CheckpointMetadata`] action, while
//!    maintaining backwards compatibility by using classic-naming that legacy readers can recognize.
//!
//! For more information on the V1/V2 specifications, see the following protocol section:
//! <https://github.com/delta-io/delta/blob/master/PROTOCOL.md#checkpoint-specs>
//!
//! ## Checkpoint Selection Logic
//! The checkpoint type is determined by whether the table supports the `v2Checkpoints` reader/writer feature:
//!
//! | Table Feature    | Resulting Checkpoint Type     |
//! |------------------|-------------------------------|
//! | No v2Checkpoints | Single-file Classic-named V1  |
//! | v2Checkpoints    | Single-file Classic-named V2  |
//!
//! ## Architecture
//!
//! - [`CheckpointWriter`] - Core component that manages checkpoint creation workflow
//! - [`CheckpointData`] - Contains the data to write and destination path information
//!
//! ## [`CheckpointWriter`]
//! Handles the actual checkpoint data generation and writing process. It is created via the
//! [`crate::table::Table::checkpoint`] method and provides the following APIs:
//! - [`CheckpointWriter::checkpoint_data`] - Returns the checkpoint data and path information
//! - [`CheckpointWriter::finalize`] - Writes the `_last_checkpoint` file
//!
//! ## Example: Writing a classic-named V1 checkpoint (no `v2Checkpoints` feature on test table)
//!
//! ```
//! use std::sync::Arc;
//! use object_store::local::LocalFileSystem;
//! use delta_kernel::{
//!     checkpoint::CheckpointData,
//!     engine::arrow_data::ArrowEngineData,
//!     engine::default::{executor::tokio::TokioBackgroundExecutor, DefaultEngine},
//!     table::Table,
//!     DeltaResult, Error,
//! };
//! use delta_kernel::arrow::array::{Int64Array, RecordBatch};
//! use delta_kernel::arrow::datatypes::{DataType, Field, Schema};
//!
//! fn mock_write_to_object_store(mut data: CheckpointData) -> DeltaResult<ArrowEngineData> {
//!     /* This should be replaced with actual object store write logic */
//!     /* For demonstration, we manually create an EngineData batch with a dummy size */
//!     let size = data.data.try_fold(0i64, |acc, r| r.map(|_| acc + 1))?;
//!     let batch = RecordBatch::try_new(
//!         Arc::new(Schema::new(vec![Field::new("sizeInBytes", DataType::Int64, false)])),
//!         vec![Arc::new(Int64Array::from(vec![size]))],
//!     )?;
//!     Ok(ArrowEngineData::new(batch))
//! }
//!
//! let engine = DefaultEngine::new(
//!     Arc::new(LocalFileSystem::new()),
//!     Arc::new(TokioBackgroundExecutor::new())
//! );
//! let table = Table::try_from_uri("./tests/data/app-txn-no-checkpoint")?;
//!
//! // Create a checkpoint writer for the table at a specific version
//! let mut writer = table.checkpoint(&engine, Some(1))?;
//!
//! // Write the checkpoint data to the object store and get the metadata
//! let metadata = mock_write_to_object_store(writer.checkpoint_data(&engine)?)?;
//!
//! /* IMPORTANT: All data must be written before finalizing the checkpoint */
//!
//!  // Finalize the checkpoint. This call will write the _last_checkpoint file
//! writer.finalize(&engine, &metadata)?;
//!
//! # Ok::<_, Error>(())
//! ```
//!
//! ## Future extensions
//! - TODO(#836): Single-file UUID-named V2 checkpoints (using `n.checkpoint.u.{json/parquet}` naming) are to be
//!   implemented in the future. The current implementation only supports classic-named V2 checkpoints.
//! - TODO(#837): Multi-file V2 checkpoints are not supported yet. The API is designed to be extensible for future
//!   multi-file support, but the current implementation only supports single-file checkpoints.
//!
//! Note: Multi-file V1 checkpoints are DEPRECATED and UNSAFE.
//!
//! [`CheckpointMetadata`]: crate::actions::CheckpointMetadata
//! [`LastCheckpointHint`]: crate::snapshot::LastCheckpointHint
use crate::actions::CHECKPOINT_METADATA_NAME;
use crate::actions::{
    schemas::GetStructField, Add, Metadata, Protocol, Remove, SetTransaction, Sidecar, ADD_NAME,
    METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME, SET_TRANSACTION_NAME, SIDECAR_NAME,
};
use crate::engine_data::FilteredEngineData;
use crate::expressions::{column_expr, Scalar};
use crate::log_replay::LogReplayProcessor;
use crate::path::ParsedLogPath;
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::snapshot::{Snapshot, LAST_CHECKPOINT_FILE_NAME};
use crate::{DeltaResult, Engine, EngineData, Error, EvaluationHandlerExtension, Expression};
use log_replay::CheckpointLogReplayProcessor;
use std::sync::atomic::{AtomicI64, Ordering};
use std::{
    sync::{Arc, LazyLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use url::Url;
mod log_replay;
#[cfg(test)]
mod tests;

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

/// Schema of metadata passed to the [`CheckpointWriter::finalize()`] method by the engine
static ENGINE_CHECKPOINT_METADATA_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| StructType::new([StructField::not_null("version", DataType::LONG)]).into());

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

// Schema of the [`CheckpointMetadata`] action that is included in V2 checkpoints
// We cannot use `CheckpointMetadata::to_schema()` as it would include the 'tags' field which
// we're not supporting yet due to the lack of map support.
static CHECKPOINT_METADATA_ACTION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    StructType::new([StructField::not_null(
        CHECKPOINT_METADATA_NAME,
        DataType::struct_type([StructField::not_null("version", DataType::LONG)]),
    )])
    .into()
});

/// Represents a single-file checkpoint, including the data to write and the target path.
pub struct CheckpointData {
    /// The URL where the checkpoint file should be written.
    pub path: Url,

    /// An iterator over the checkpoint data to be written to the file.
    pub data: Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>>>,
}

/// Manages the checkpoint writing process for tables
///
/// The [`CheckpointWriter`] orchestrates creating checkpoint data, and finalizing the
/// checkpoint by writing the `_last_checkpoint` file.
pub struct CheckpointWriter {
    /// Reference to the snapshot of the table being checkpointed
    pub(crate) snapshot: Arc<Snapshot>,
    /// Note: `Arc<AtomicI64>` provides shared mutability for our counters, allowing the
    /// returned actions iterator from `.checkpoint_data()` to update the counters,
    /// and the [`CheckpointWriter`] to read them during `.finalize()`
    /// Counter for total actions included in the checkpoint
    pub(crate) total_actions_counter: Arc<AtomicI64>,
    /// Counter for Add actions included in the checkpoint
    pub(crate) add_actions_counter: Arc<AtomicI64>,
}

impl CheckpointWriter {
    /// Creates a new CheckpointWriter with the provided checkpoint data and counters
    pub(crate) fn new(snapshot: Arc<Snapshot>) -> Self {
        Self {
            snapshot,
            total_actions_counter: Arc::new(AtomicI64::new(0)),
            add_actions_counter: Arc::new(AtomicI64::new(0)),
        }
    }

    /// Retrieves the checkpoint data and path information
    ///
    /// This method is the core of the checkpoint generation process. It:
    /// 1. Determines whether to write a V1 or V2 checkpoint based on the table's
    ///    `v2Checkpoints` feature support
    /// 2. Reads actions from the log segment using the checkpoint read schema
    /// 3. Filters and deduplicates actions for the checkpoint
    /// 4. Chains the checkpoint metadata action if writing a V2 spec checkpoint
    ///    (i.e., if `v2Checkpoints` feature is supported by table)
    /// 5. Generates the appropriate checkpoint path
    ///
    /// # Important: The returned data should be written to persistent storage by the
    /// caller before calling `finalize()` otherwise data loss may occur.
    ///
    /// # Returns: [`CheckpointData`] containing the checkpoint path and data to write
    pub fn checkpoint_data(&mut self, engine: &dyn Engine) -> DeltaResult<CheckpointData> {
        let is_v2_checkpoints_supported = self
            .snapshot
            .table_configuration()
            .is_v2_checkpoint_supported();

        let actions = self.snapshot.log_segment().read_actions(
            engine,
            CHECKPOINT_ACTIONS_SCHEMA.clone(),
            CHECKPOINT_ACTIONS_SCHEMA.clone(),
            None,
        )?;

        // Create iterator over actions for checkpoint data
        let checkpoint_data = CheckpointLogReplayProcessor::new(
            self.total_actions_counter.clone(),
            self.add_actions_counter.clone(),
            self.deleted_file_retention_timestamp()?,
        )
        .process_actions_iter(actions);

        let version = self.snapshot.version().try_into().map_err(|e| {
            Error::checkpoint_writer(format!(
                "Failed to convert checkpoint version from u64 {} to i64: {}",
                self.snapshot.version(),
                e
            ))
        })?;

        // Chain the checkpoint metadata action if using V2 checkpoints
        let chained = checkpoint_data.chain(self.create_checkpoint_metadata_batch(
            version,
            engine,
            is_v2_checkpoints_supported,
        )?);

        let checkpoint_path = ParsedLogPath::new_classic_parquet_checkpoint(
            self.snapshot.table_root(),
            self.snapshot.version(),
        )?;

        Ok(CheckpointData {
            path: checkpoint_path.location,
            data: Box::new(chained),
        })
    }

    /// Finalizes the checkpoint writing process by creating the `_last_checkpoint` file
    ///
    /// The `_last_checkpoint` file is a metadata file that contains information about the
    /// last checkpoint created for the table. It is used as a hint for the engine to quickly
    /// locate the last checkpoint and avoid full log replay when reading the table.
    ///
    /// # Important
    /// This method must only be called AFTER successfully writing all checkpoint data to storage.
    /// Failure to do so may result in data loss.
    ///
    /// # Parameters
    /// - `engine`: The engine used for writing the `_last_checkpoint` file
    /// - `metadata`: A single-row, single-column [`EngineData`] batch containing:
    ///   - `sizeInBytes` (i64): The size of the written checkpoint file
    ///
    /// # Returns: [`variant@Ok`] if the `_last_checkpoint` file was written successfully
    pub fn finalize(self, engine: &dyn Engine, metadata: &dyn EngineData) -> DeltaResult<()> {
        let version = self.snapshot.version().try_into().map_err(|e| {
            Error::checkpoint_writer(format!(
                "Failed to convert checkpoint version from u64 {} to i64: {}",
                self.snapshot.version(),
                e
            ))
        })?;

        // Ordering does not matter as there are no other threads modifying this counter
        // at this time (since the checkpoint data iterator has been consumed)
        let checkpoint_metadata = create_last_checkpoint_data(
            engine,
            metadata,
            version,
            self.total_actions_counter.load(Ordering::Relaxed),
            self.add_actions_counter.load(Ordering::Relaxed),
        )?;

        let last_checkpoint_path = self
            .snapshot
            .log_segment()
            .log_root
            .join(LAST_CHECKPOINT_FILE_NAME)?;

        engine.json_handler().write_json_file(
            &last_checkpoint_path,
            Box::new(std::iter::once(Ok(checkpoint_metadata))),
            true, // overwrite the last checkpoint file
        )?;

        Ok(())
    }

    /// Creates the checkpoint metadata action for V2 checkpoints.
    ///
    /// For V2 checkpoints, this function generates the [`CheckpointMetadata`] action
    /// that must be included in the V2 spec checkpoint file. This action contains metadata
    /// about the checkpoint, particularly its version. For V1 checkpoints, this function
    /// returns `None`, as the V1 checkpoint schema does not include this action type.
    ///
    /// # Implementation Details
    ///
    /// The function creates a single-row [`EngineData`] batch containing only the
    /// version field of the [`CheckpointMetadata`] action. Future implementations will
    /// include the additional metadata field `tags` when map support is added.
    ///
    /// # Returns:
    /// A [`FilteredEngineData`] batch including the single-row [`EngineData`] batch along with
    /// an accompanying selection vector with a single `true` value, indicating the action in
    /// batch should be included in the checkpoint.
    fn create_checkpoint_metadata_batch(
        &self,
        version: i64,
        engine: &dyn Engine,
        is_v2_checkpoint: bool,
    ) -> DeltaResult<Option<DeltaResult<FilteredEngineData>>> {
        if !is_v2_checkpoint {
            return Ok(None);
        }
        let values: &[Scalar] = &[version.into()];

        let checkpoint_metadata_batch = engine
            .evaluation_handler()
            .create_one(CHECKPOINT_METADATA_ACTION_SCHEMA.clone(), values)?;

        let result = FilteredEngineData {
            data: checkpoint_metadata_batch,
            selection_vector: vec![true], // Include the action in the checkpoint
        };

        // Ordering does not matter as there are no other threads modifying this counter
        // at this time (since we have not yet returned the iterator which performs the action counting)
        self.total_actions_counter.fetch_add(1, Ordering::Relaxed);

        Ok(Some(Ok(result)))
    }

    /// Calculates the cutoff timestamp for deleted file cleanup.
    ///
    /// This function determines the minimum timestamp before which deleted files
    /// will be permanently removed during VACUUM operations, based on the table's
    /// `deleted_file_retention_duration` property.
    ///
    /// Returns the cutoff timestamp in milliseconds since epoch, matching
    /// the remove action's `deletion_timestamp` field format for comparison.
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
        retention_duration.unwrap_or_else(|| Duration::from_secs(60 * 60 * 24 * 7));

    // Convert to milliseconds for remove action deletion_timestamp comparison
    let now_ms: i64 = now_duration
        .as_millis()
        .try_into()
        .map_err(|_| Error::checkpoint_writer("Current timestamp exceeds i64 millisecond range"))?;

    let retention_ms: i64 = retention_duration.as_millis().try_into().map_err(|_| {
        Error::checkpoint_writer("Retention duration exceeds i64 millisecond range")
    })?;

    // Simple subtraction - will produce negative values if retention > now
    Ok(now_ms - retention_ms)
}

/// Creates the data for the `_last_checkpoint` file containing checkpoint metadata
///
/// # Parameters
/// - `engine`: Engine for data processing
/// - `metadata`: Single-row data containing `sizeInBytes` (i64)
/// - `version`: Table version number
/// - `total_actions_counter`: Total actions count
/// - `total_add_actions_counter`: Add actions count
///
/// # Returns
/// A new [`EngineData`] batch with the `_last_checkpoint` fields:
/// - `version` (i64, required): Table version number
/// - `size` (i64, required): Total actions count
/// - `parts` (i64, optional): Always 1 for single-file checkpoints
/// - `sizeInBytes` (i64, optional): Size of checkpoint file in bytes
/// - `numOfAddFiles` (i64, optional): Number of Add actions
///
/// TODO(#838) Add `checksum` field to the `_last_checkpoint` file
/// TODO(#839) Add `checkpoint_schema` field to the `_last_checkpoint` file
fn create_last_checkpoint_data(
    engine: &dyn Engine,
    metadata: &dyn EngineData,
    version: i64,
    total_actions_counter: i64,
    add_actions_counter: i64,
) -> DeltaResult<Box<dyn EngineData>> {
    // Validate metadata has exactly one row
    if metadata.len() != 1 {
        return Err(Error::checkpoint_writer(format!(
            "Engine-collected checkpoint metadata should have exactly one row, found {}",
            metadata.len()
        )));
    }

    let last_checkpoint_exprs = [
        Expression::literal(version),
        Expression::literal(total_actions_counter),
        Expression::literal(1i64), // Single-file checkpoint
        column_expr!("sizeInBytes"),
        Expression::literal(add_actions_counter),
        // TODO(#838): Include the checksum here
        // TODO(#839): Include the schema here
    ];
    let last_checkpoint_expr = Expression::struct_from(last_checkpoint_exprs);

    let last_checkpoint_metadata_evaluator = engine.evaluation_handler().new_expression_evaluator(
        ENGINE_CHECKPOINT_METADATA_SCHEMA.clone(),
        last_checkpoint_expr,
        LAST_CHECKPOINT_SCHEMA.clone().into(),
    );

    last_checkpoint_metadata_evaluator.evaluate(metadata)
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
    use crate::engine::{arrow_data::ArrowEngineData, sync::SyncEngine};
    use crate::Table;
    use arrow_53::array::Int64Array;
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
        let arrow_engine_data = ArrowEngineData::try_from_engine_data(checkpoint_data.data)?;
        let record_batch = arrow_engine_data.record_batch();

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
        assert_eq!(writer.total_actions_counter.load(Ordering::Relaxed), 0);

        Ok(())
    }

    #[test]
    fn test_create_last_checkpoint_metadata() -> DeltaResult<()> {
        // Setup test data
        let size_in_bytes: i64 = 1024 * 1024; // 1MB
        let version = 10;
        let total_actions_counter = 100;
        let add_actions_counter = 75;
        let engine = SyncEngine::new();

        // Create engine metadata with `size_in_bytes`
        let schema = ArrowSchema::new(vec![Field::new("sizeInBytes", ArrowDataType::Int64, false)]);
        let size_array = Int64Array::from(vec![size_in_bytes]);
        let record_batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(size_array)])?;
        let metadata = ArrowEngineData::new(record_batch);

        // Create last checkpoint metadata
        let last_checkpoint_batch = create_last_checkpoint_data(
            &engine,
            &metadata,
            version,
            total_actions_counter,
            add_actions_counter,
        )?;

        // Verify the underlying EngineData contains the expected LastCheckpointInfo schema and data
        let arrow_engine_data = ArrowEngineData::try_from_engine_data(last_checkpoint_batch)?;
        let record_batch = arrow_engine_data.record_batch();

        // Build the expected RecordBatch
        let expected_schema = Arc::new(Schema::new(vec![
            Field::new("version", DataType::Int64, false),
            Field::new("size", DataType::Int64, false),
            Field::new("parts", DataType::Int64, true),
            Field::new("sizeInBytes", DataType::Int64, true),
            Field::new("numOfAddFiles", DataType::Int64, true),
        ]));
        let expected = RecordBatch::try_new(
            expected_schema,
            vec![
                create_array!(Int64, [version]),
                create_array!(Int64, [total_actions_counter]),
                create_array!(Int64, [1]),
                create_array!(Int64, [size_in_bytes]),
                create_array!(Int64, [add_actions_counter]),
            ],
        )
        .unwrap();

        assert_eq!(*record_batch, expected);
        Ok(())
    }

    #[test]
    fn test_create_last_checkpoint_metadata_with_invalid_batch() -> DeltaResult<()> {
        let engine = SyncEngine::new();

        // Create engine metadata with the wrong schema
        let schema = ArrowSchema::new(vec![Field::new("wrongField", ArrowDataType::Int64, false)]);
        let size_array = Int64Array::from(vec![0]);
        let record_batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(size_array)])
            .expect("Failed to create record batch");
        let metadata = Box::new(ArrowEngineData::new(record_batch));

        // This should fail because the schema does not match the expected schema
        let res = create_last_checkpoint_data(&engine, &*metadata, 0, 0, 0);

        // Verify that an error is returned
        assert!(res.is_err());
        Ok(())
    }
}
