//! # Delta Kernel Checkpoint API
//!
//! This module provides functionality for writing single-file checkpoints in Delta tables.
//!
//! 1. Single-file Classic-named V1 Checkpoint - For legacy tables without v2Checkpoints feature
//! 2. Single-file Classic-named V2 Checkpoint - For backwards compatibility with v2Checkpoints feature
//! 3. Single-file UUID-named V2 Checkpoint - Recommended for small to medium tables with v2Checkpoints feature
//!
//! The API is designed with a builder pattern for configuring and creating checkpoint writers.
//!
//! # Example
//! ```ignore
//! let path = "./tests/data/app-txn-no-checkpoint";
//! let engine = Arc::new(SyncEngine::new());
//! let table = Table::try_from_uri(path)?;
//! // Create a checkpoint builder for the table at a specific version
//! let builder = table.checkpoint(&engine, Some(2))?;
//! // Configure the builder (optional)
//! let writer = builder.with_classic_naming(true);
//! // Build the checkpoint writer
//! let writer = builder.build(&engine)?;
//! // Get the checkpoint data and path
//! let checkpoint_data = writer.get_checkpoint_info()?;
//! /* Engine writes data to file path and collects metadata: (path, bytes, timestamp) */
//! /* All checkpoint data must be written before calling .finalize_checkpoint() */
//! writer.finalize_checkpoint()?;
//! ```
use log_replay::{checkpoint_actions_iter, CheckpointData};
use std::{
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc, LazyLock,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use url::Url;

use crate::actions::schemas::GetStructField;
use crate::expressions::column_expr;
use crate::schema::{SchemaRef, StructType};
use crate::{
    actions::{
        Add, Metadata, Protocol, Remove, SetTransaction, Sidecar, ADD_NAME, METADATA_NAME,
        PROTOCOL_NAME, REMOVE_NAME, SET_TRANSACTION_NAME, SIDECAR_NAME,
    },
    path::ParsedLogPath,
    snapshot::Snapshot,
    DeltaResult, Engine, EngineData, Error, Expression, Version,
};
pub mod log_replay;
#[cfg(test)]
mod tests;

/// Schema definition for the _last_checkpoint file
pub(crate) static CHECKPOINT_METADATA_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new(vec![
        <Version>::get_struct_field("version"),
        <i64>::get_struct_field("size"),
        Option::<usize>::get_struct_field("parts"),
        Option::<i64>::get_struct_field("sizeInBytes"),
        Option::<i64>::get_struct_field("numOfAddFiles"),
        // Option::<Schema>::get_struct_field("checkpoint_schema"), TODO: Schema
        // Option::<String>::get_struct_field("checksum"), TODO: Checksum
    ]))
});

/// Get the expected schema for the _last_checkpoint file
pub fn get_checkpoint_metadata_schema() -> &'static SchemaRef {
    &CHECKPOINT_METADATA_SCHEMA
}

/// Read schema definition for collecting checkpoint actions
static CHECKPOINT_READ_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
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

/// Returns the read schema to collect checkpoint actions
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
#[cfg_attr(not(feature = "developer-visibility"), visibility::make(pub(crate)))]
fn get_checkpoint_read_schema() -> &'static SchemaRef {
    &CHECKPOINT_READ_SCHEMA
}

/// Contains the path and data for a single-file checkpoint.
///
/// This struct holds all the necessary information for writing a checkpoint file,
/// including the destination path and the iterator over checkpoint actions.
pub struct SingleFileCheckpointData {
    /// The target URL where the checkpoint file will be written
    pub path: Url,

    /// Iterator over checkpoint actions to be written to the file
    pub data: Box<dyn Iterator<Item = DeltaResult<CheckpointData>>>,
}

/// Writer for creating checkpoint files in Delta tables.
///
/// The CheckpointWriter orchestrates the process of writing checkpoint data to storage.
/// It manages the one-time consumption of checkpoint data and tracks statistics
/// about the actions included in the checkpoint.
pub struct CheckpointWriter {
    /// Using Option to enforce single consumption at compile time
    single_file_checkpoint_data: Option<SingleFileCheckpointData>,

    /// Counter for the total number of actions in the checkpoint
    total_actions_counter: Arc<AtomicI64>,

    /// Counter for add file actions specifically
    total_add_actions_counter: Arc<AtomicI64>,

    /// Version of the checkpoint
    version: Version,

    /// Number of parts of the checkpoint
    parts: usize,

    /// Path to table's log
    log_root: Url,
}

impl CheckpointWriter {
    /// Creates a new CheckpointWriter with the provided checkpoint data and counters
    fn new(
        single_file_checkpoint_data: Option<SingleFileCheckpointData>,
        total_actions_counter: Arc<AtomicI64>,
        total_add_actions_counter: Arc<AtomicI64>,
        version: Version,
        parts: usize,
        log_root: Url,
    ) -> Self {
        Self {
            single_file_checkpoint_data,
            total_actions_counter,
            total_add_actions_counter,
            version,
            parts,
            log_root,
        }
    }

    /// Retrieves the checkpoint data and path information
    ///
    /// This method takes ownership of the checkpoint data, ensuring it can
    /// only be consumed once. It returns an error if the data has already
    /// been consumed.
    pub fn get_checkpoint_info(&mut self) -> DeltaResult<SingleFileCheckpointData> {
        self.single_file_checkpoint_data
            .take()
            .ok_or_else(|| Error::generic("Checkpoint data already consumed"))
    }

    /// Finalizes the checkpoint writing process
    ///
    /// This method should be only called AFTER writing all checkpoint data to
    /// ensure proper completion of the checkpoint operation, which includes
    /// writing the _last_checkpoint file.
    ///
    /// Metadata is a single-row EngineData batch with {size_in_bytes: i64}
    /// Given the engine collected checkpoint metadata we want to extend
    /// the EngineData batch with the remaining fields for the `_last_checkpoint`
    /// file.
    pub fn finalize_checkpoint(
        self,
        engine: &dyn Engine,
        metadata: &dyn EngineData,
    ) -> DeltaResult<()> {
        // Prepare the checkpoint metadata
        let checkpoint_metadata = self.prepare_last_checkpoint_metadata(engine, metadata)?;

        // Write the metadata to _last_checkpoint.json
        let last_checkpoint_path = self.log_root.join("_last_checkpoint.json")?;

        engine.get_json_handler().write_json_file(
            &last_checkpoint_path,
            Box::new(std::iter::once(Ok(checkpoint_metadata))),
            true, // overwrite the last checkpoint file
        )?;

        Ok(())
    }

    /// Prepares the _last_checkpoint metadata batch
    ///
    /// This method validates and transforms the engine-provided metadata into
    /// the complete checkpoint metadata including counters and versioning information.
    ///
    /// Refactored into a separate method to facilitate testing.
    fn prepare_last_checkpoint_metadata(
        &self,
        engine: &dyn Engine,
        metadata: &dyn EngineData,
    ) -> DeltaResult<Box<dyn EngineData>> {
        // Validate metadata has exactly one row
        if metadata.len() != 1 {
            return Err(Error::Generic(format!(
                "Engine checkpoint metadata should have exactly one row, found {}",
                metadata.len()
            )));
        }

        // Create expression for transforming the metadata
        let last_checkpoint_exprs = [
            Expression::literal(self.version),
            Expression::literal(self.total_actions_counter.load(Ordering::SeqCst)),
            Expression::literal(self.parts),
            column_expr!("sizeInBytes"),
            Expression::literal(self.total_add_actions_counter.load(Ordering::SeqCst)),
        ];
        let last_checkpoint_expr = Expression::struct_from(last_checkpoint_exprs);

        // Get schemas for transformation
        let last_checkpoint_schema = get_checkpoint_metadata_schema();
        let engine_metadata_schema = last_checkpoint_schema.project_as_struct(&["sizeInBytes"])?;

        // Create the evaluator for the transformation
        let last_checkpoint_metadata_evaluator = engine.get_expression_handler().get_evaluator(
            engine_metadata_schema.into(),
            last_checkpoint_expr,
            last_checkpoint_schema.clone().into(),
        );

        // Transform the metadata
        last_checkpoint_metadata_evaluator.evaluate(metadata)
    }
}

/// Builder for configuring and creating CheckpointWriter instances
///
/// The CheckpointBuilder provides an interface for configuring checkpoint
/// generation. It handles table feature detection and enforces compatibility
/// between configuration options and table features.
pub struct CheckpointBuilder {
    /// The table snapshot from which to create the checkpoint
    snapshot: Snapshot,

    /// Whether to use classic naming for the checkpoint file
    with_classic_naming: bool,
}

impl CheckpointBuilder {
    /// Creates a new CheckpointBuilder with the given snapshot
    pub(crate) fn new(snapshot: Snapshot) -> Self {
        Self {
            snapshot,
            with_classic_naming: false,
        }
    }

    /// Configures the builder to use classic naming scheme
    ///
    /// Classic naming is required for V1 checkpoints and optional for V2 checkpoints.
    /// For V2 checkpoints, the default is UUID naming unless this method is called.
    pub fn with_classic_naming(mut self, with_classic_naming: bool) -> Self {
        self.with_classic_naming = with_classic_naming;
        self
    }

    /// Builds a CheckpointWriter based on the configuration
    ///
    /// This method validates the configuration against table features and creates
    /// a CheckpointWriter for the appropriate checkpoint type. It performs protocol
    /// table feature checks to determine if v2Checkpoints are supported.
    ///
    /// # Arguments
    /// * `engine` - The engine implementation for data operations
    ///
    /// # Returns
    /// * `DeltaResult<CheckpointWriter>` - A configured checkpoint writer on success,
    ///   or an error if the configuration is incompatible with table features
    pub fn build(self, engine: &dyn Engine) -> DeltaResult<CheckpointWriter> {
        let v2_checkpoints_supported = self
            .snapshot
            .table_configuration()
            .is_v2_checkpoint_supported();

        let deleted_file_retention_timestamp = self.deleted_file_retention_timestamp()?;

        // Create counters for tracking actions
        let total_actions_counter = Arc::new(AtomicI64::new(0));
        let total_add_actions_counter = Arc::new(AtomicI64::new(0));

        // Create iterator over actions for checkpoint data
        let checkpoint_data = checkpoint_actions_iter(
            self.replay_for_checkpoint_data(engine)?,
            total_actions_counter.clone(),
            total_add_actions_counter.clone(),
            deleted_file_retention_timestamp,
        );

        // Generate checkpoint path based on builder configuration
        // Classic naming is required for V1 checkpoints and optional for V2 checkpoints
        let checkpoint_path = if self.with_classic_naming || !v2_checkpoints_supported {
            ParsedLogPath::new_classic_parquet_checkpoint(
                self.snapshot.table_root(),
                self.snapshot.version(),
            )?
        } else {
            ParsedLogPath::new_uuid_parquet_checkpoint(
                self.snapshot.table_root(),
                self.snapshot.version(),
            )?
        };

        let data = SingleFileCheckpointData {
            data: Box::new(checkpoint_data),
            path: checkpoint_path.location,
        };

        Ok(CheckpointWriter::new(
            Some(data),
            total_actions_counter,
            total_add_actions_counter,
            self.snapshot.version(),
            1,
            self.snapshot.log_segment().log_root.clone(),
        ))
    }

    /// Prepares the iterator over actions for checkpoint creation
    ///
    /// This method is factored out to facilitate testing and returns an iterator
    /// over all actions to be included in the checkpoint.
    fn replay_for_checkpoint_data(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send> {
        let read_schema = get_checkpoint_read_schema();

        self.snapshot.log_segment().read_actions(
            engine,
            read_schema.clone(),
            read_schema.clone(),
            None,
        )
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
    pub(crate) fn deleted_file_retention_timestamp(&self) -> DeltaResult<i64> {
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
    use crate::arrow::array::Int64Array;
    use crate::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use crate::arrow::record_batch::RecordBatch;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::arrow_expression::ArrowExpressionHandler;
    use crate::{ExpressionHandler, FileSystemClient, JsonHandler, ParquetHandler};
    use arrow_53::json::LineDelimitedWriter;
    use std::sync::{atomic::AtomicI64, Arc};
    use std::time::Duration;
    use url::Url;

    // Helper to serialize and extract the _last_checkpoint JSON for verification
    fn as_json(data: Box<dyn EngineData>) -> serde_json::Value {
        let record_batch: RecordBatch = data
            .into_any()
            .downcast::<ArrowEngineData>()
            .unwrap()
            .into();

        let buf = Vec::new();
        let mut writer = LineDelimitedWriter::new(buf);
        writer.write_batches(&[&record_batch]).unwrap();
        writer.finish().unwrap();
        let buf = writer.into_inner();

        serde_json::from_slice(&buf).unwrap()
    }

    // TODO(seb): Merge with other definitions and move to a common test module
    pub(crate) struct ExprEngine(Arc<dyn ExpressionHandler>);

    impl ExprEngine {
        pub(crate) fn new() -> Self {
            ExprEngine(Arc::new(ArrowExpressionHandler))
        }
    }

    impl Engine for ExprEngine {
        fn get_expression_handler(&self) -> Arc<dyn ExpressionHandler> {
            self.0.clone()
        }

        fn get_json_handler(&self) -> Arc<dyn JsonHandler> {
            unimplemented!()
        }

        fn get_parquet_handler(&self) -> Arc<dyn ParquetHandler> {
            unimplemented!()
        }

        fn get_file_system_client(&self) -> Arc<dyn FileSystemClient> {
            unimplemented!()
        }
    }

    /// Creates a mock engine metadata batch with size_in_bytes field
    fn create_engine_metadata(size_in_bytes: i64) -> Box<dyn EngineData> {
        // Create Arrow schema with size_in_bytes field
        let schema = ArrowSchema::new(vec![Field::new("sizeInBytes", ArrowDataType::Int64, false)]);

        let size_array = Int64Array::from(vec![size_in_bytes]);
        let record_batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(size_array)])
            .expect("Failed to create record batch");
        Box::new(ArrowEngineData::new(record_batch))
    }

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

    #[test]
    fn test_prepare_last_checkpoint_metadata() -> DeltaResult<()> {
        // Setup test data
        let size_in_bytes: i64 = 1024 * 1024; // 1MB
        let version: Version = 10;
        let parts: usize = 3;
        let total_actions_counter = Arc::new(AtomicI64::new(100));
        let total_add_actions_counter = Arc::new(AtomicI64::new(75));

        let log_root = Url::parse("memory://test-table/_delta_log/").unwrap();
        let engine = ExprEngine::new();

        // Create engine metadata with size_in_bytes
        let metadata = create_engine_metadata(size_in_bytes);

        // Create checkpoint writer
        let writer = CheckpointWriter::new(
            None, // We don't need checkpoint data for this test
            total_actions_counter.clone(),
            total_add_actions_counter.clone(),
            version,
            parts,
            log_root,
        );

        // Call the method under test
        let last_checkpoint_batch = writer.prepare_last_checkpoint_metadata(&engine, &*metadata)?;

        // Convert to JSON for easier verification
        let json = as_json(last_checkpoint_batch);

        // Verify the values match our expectations
        assert_eq!(json["version"], version);
        assert_eq!(json["size"], total_actions_counter.load(Ordering::Relaxed));
        assert_eq!(json["parts"], parts as i64);
        assert_eq!(json["sizeInBytes"], size_in_bytes);
        assert_eq!(
            json["numOfAddFiles"],
            total_add_actions_counter.load(Ordering::Relaxed)
        );

        Ok(())
    }

    #[test]
    fn test_prepare_last_checkpoint_metadata_with_empty_batch() {
        // Setup test data
        let version: Version = 10;
        let parts: usize = 3;
        let total_actions_counter = Arc::new(AtomicI64::new(100));
        let total_add_actions_counter = Arc::new(AtomicI64::new(75));

        let log_root = Url::parse("memory://test-table/_delta_log/").unwrap();
        let engine = ExprEngine::new();

        // Create empty metadata (no rows)
        let empty_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "sizeInBytes",
            ArrowDataType::Int64,
            false,
        )]));
        let empty_batch = RecordBatch::try_new(
            empty_schema,
            vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
        )
        .expect("Failed to create empty batch");
        let empty_metadata = ArrowEngineData::new(empty_batch);

        // Create checkpoint writer
        let writer = CheckpointWriter::new(
            None,
            total_actions_counter,
            total_add_actions_counter,
            version,
            parts,
            log_root,
        );

        // Call the method under test - should fail with InvalidCommitInfo
        let result = writer.prepare_last_checkpoint_metadata(&engine, &empty_metadata);
        assert!(result.is_err());

        match result {
            Err(Error::Generic(e)) => {
                assert_eq!(
                    e,
                    "Engine checkpoint metadata should have exactly one row, found 0"
                );
            }
            _ => panic!("Should have failed with error"),
        }
    }

    #[test]
    fn test_prepare_last_checkpoint_metadata_with_multiple_rows() {
        // Setup test data
        let version: Version = 10;
        let parts: usize = 1;
        let total_actions_counter = Arc::new(AtomicI64::new(50));
        let total_add_actions_counter = Arc::new(AtomicI64::new(30));

        // Create a log root URL
        let log_root = Url::parse("memory://test-table/_delta_log/").unwrap();

        // Create engine
        let engine = ExprEngine::new();

        // Create metadata with multiple rows
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "sizeInBytes",
            ArrowDataType::Int64,
            false,
        )]));
        let multi_row_batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1024, 2048]))])
                .expect("Failed to create multi-row batch");
        let multi_row_metadata = ArrowEngineData::new(multi_row_batch);

        // Create checkpoint writer
        let writer = CheckpointWriter::new(
            None,
            total_actions_counter,
            total_add_actions_counter,
            version,
            parts,
            log_root,
        );

        // Call the method under test - should fail with InvalidCommitInfo
        let result = writer.prepare_last_checkpoint_metadata(&engine, &multi_row_metadata);
        assert!(result.is_err());

        match result {
            Err(Error::Generic(e)) => {
                assert_eq!(
                    e,
                    "Engine checkpoint metadata should have exactly one row, found 2"
                );
            }
            _ => panic!("Should have failed with error"),
        }
    }
}
